import {
  apiError,
  isAuthorized,
  isUuid,
  jobPath,
  json,
  readCreateJobBody,
  readCallbackBody,
  sha256,
} from "./http.ts";
import { appConfig } from "./generated-config.ts";
import { HistoryError } from "./history.ts";
import type { Env, JobView } from "./types.ts";
import type {
  InteractionCallbackResult,
  TokenBudgetCallbackResult,
} from "./container.ts";
import type { BudgetAvailability } from "./workflow-budget.ts";
import { submitWorkflowJob } from "./job-submission.ts";
import {
  loadWorkflowBundleArchive,
  loadActiveWorkflowBundle,
} from "./workflow.ts";
import { handleSlackInteraction } from "./slack-interactions.ts";
import {
  noOpTracer,
  recordHttpResult,
  setFlowTraceAttributes,
  setSpanAttributes,
  spanNames,
  traceAsync,
  type TraceSpan,
  type Tracer,
} from "./tracing.ts";

function containerStub(env: Env, jobId: string) {
  return env.AGENT_CONTAINER.get(env.AGENT_CONTAINER.idFromName(jobId));
}

function tokenBudgetRejection(budget: BudgetAvailability): {
  message: string;
  details: Record<string, unknown>;
} {
  const counts =
    budget.limit === null
      ? {}
      : {
          used_tokens: budget.used,
          limit_tokens: budget.limit,
          overage_tokens: Math.max(0, budget.used - budget.limit),
        };
  if (budget.reason === "indeterminate") {
    return {
      message:
        "The workflow token budget is unavailable because token usage could not be determined",
      details: counts,
    };
  }
  if (budget.reason === "suspended_jobs") {
    return {
      message: "The workflow token budget is reserved for older suspended jobs",
      details: counts,
    };
  }
  const used = budget.used.toLocaleString("en-US");
  const limit = (budget.limit ?? 0).toLocaleString("en-US");
  const overage = Math.max(0, budget.used - (budget.limit ?? 0));
  const message =
    overage > 0
      ? `The workflow token budget is exhausted by ${overage.toLocaleString("en-US")} tokens (${used} used of ${limit})`
      : `The workflow token budget has reached its limit (${used} used of ${limit})`;
  return { message, details: counts };
}

/** Checks bundle, selected credentials, storage and the public callback origin. */
export async function preflightResponse(env: Env): Promise<Response> {
  try {
    const bundle = await loadActiveWorkflowBundle(env);
    await loadWorkflowBundleArchive(env, bundle.manifest);
    if (!appConfig.runnerUrl)
      throw new Error("RUNNER_URL must identify the public callback origin");
    const selected = new Set(
      bundle.manifest.workflow.steps
        .filter((step) => "prompt" in step)
        .map((step) => ("provider" in step ? step.provider : "")),
    );
    const names = new Set<string>([appConfig.authSecretName]);
    for (const injection of appConfig.credentialInjections) {
      if (
        injection.targetKind === "provider" &&
        !selected.has(injection.targetName)
      )
        continue;
      if (injection.upstream.kind === "static")
        names.add(injection.upstream.secret);
      else {
        const credential = appConfig.oauthCredentials[injection.credentialName];
        if (credential) {
          names.add(credential.clientIdSecret);
          if (credential.clientAuth.method !== "none")
            names.add(credential.clientAuth.secret);
          if (credential.grant.type === "refresh_token")
            names.add(credential.grant.refreshTokenSecret);
        }
      }
    }
    if (appConfig.interactions.provider === "slack") {
      names.add(appConfig.interactions.botTokenSecret);
      names.add(appConfig.interactions.signingSecret);
    }
    const values = env as unknown as Record<string, unknown>;
    const missing = [...names].filter(
      (name) =>
        typeof values[name] !== "string" || !(values[name] as string).trim(),
    );
    if (missing.length)
      throw new Error(`Missing Worker secrets: ${missing.join(", ")}`);
    const probeKey = `preflight/${crypto.randomUUID()}`;
    try {
      await env.RUNNER_STORAGE.put(probeKey, "ready");
      if (!(await env.RUNNER_STORAGE.get(probeKey)))
        throw new Error("R2 read probe failed");
    } finally {
      await env.RUNNER_STORAGE.delete(probeKey);
    }
    return json({
      status: "ready",
      workflow: bundle.manifest.workflow.name,
      workflow_version: bundle.manifest.digest,
      callback_url: appConfig.runnerUrl,
      history: { storage: "r2" },
    });
  } catch (error) {
    return apiError(
      503,
      "preflight_failed",
      error instanceof Error ? error.message : "Runner preflight failed",
      {},
      { "retry-after": "30" },
    );
  }
}

interface RequestTrace {
  name: string;
  route?: string;
  jobId?: string;
}

/** Resolves a bounded operation name and route template without using raw URL paths. */
function requestTrace(request: Request): RequestTrace {
  const url = new URL(request.url);
  const callback = /^\/internal\/v1\/jobs\/([^/]+)\/events$/.exec(url.pathname);
  if (callback && isUuid(callback[1]!)) {
    return {
      name: spanNames.eventsReceive,
      route: "/internal/v1/jobs/:job_id/events",
      jobId: callback[1],
    };
  }
  const interactionCallback =
    /^\/internal\/v1\/jobs\/([^/]+)\/interactions$/.exec(url.pathname);
  if (interactionCallback && isUuid(interactionCallback[1]!)) {
    return {
      name: spanNames.request,
      route: "/internal/v1/jobs/:job_id/interactions",
      jobId: interactionCallback[1],
    };
  }
  const budgetCallback = /^\/internal\/v1\/jobs\/([^/]+)\/token-usage$/.exec(
    url.pathname,
  );
  if (budgetCallback && isUuid(budgetCallback[1]!)) {
    return {
      name: spanNames.request,
      route: "/internal/v1/jobs/:job_id/token-usage",
      jobId: budgetCallback[1],
    };
  }
  if (url.pathname === "/v1/interactions/slack")
    return { name: spanNames.request, route: "/v1/interactions/slack" };
  if (url.pathname === "/v1/workflow-budget/reconcile")
    return { name: spanNames.request, route: "/v1/workflow-budget/reconcile" };
  if (url.pathname === "/v1/preflight")
    return { name: spanNames.preflight, route: "/v1/preflight" };
  if (url.pathname === "/v1/jobs")
    return { name: spanNames.jobCreate, route: "/v1/jobs" };
  const resource =
    /^\/v1\/jobs\/([^/]+)\/(events|traces|metrics|artifacts(?:\/.*)?)$/.exec(
      url.pathname,
    );
  if (resource && isUuid(resource[1]!))
    return {
      name: spanNames.jobGet,
      route: `/v1/jobs/:job_id/${resource[2]!.startsWith("artifacts") ? "artifacts/:path" : resource[2]}`,
      jobId: resource[1],
    };
  const job = /^\/v1\/jobs\/([^/]+)$/.exec(url.pathname);
  if (job && isUuid(job[1]!)) {
    return {
      name:
        request.method === "DELETE" ? spanNames.jobCancel : spanNames.jobGet,
      route: "/v1/jobs/:job_id",
      jobId: job[1],
    };
  }
  return { name: spanNames.request };
}

/** Routes authenticated readiness, job creation, lookup, and cancellation requests. */
async function dispatchRequest(
  request: Request,
  env: Env,
  span: TraceSpan,
): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/v1/interactions/slack") {
    if (request.method !== "POST")
      return apiError(
        405,
        "method_not_allowed",
        "Use POST /v1/interactions/slack",
        {},
        { Allow: "POST" },
      );
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .startsWith("application/x-www-form-urlencoded")
    ) {
      return apiError(
        415,
        "unsupported_media_type",
        "Content-Type must be application/x-www-form-urlencoded",
      );
    }
    return handleSlackInteraction(request, env);
  }
  const callbackMatch = /^\/internal\/v1\/jobs\/([^/]+)\/events$/.exec(
    url.pathname,
  );
  if (callbackMatch && request.method === "POST" && isUuid(callbackMatch[1]!)) {
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .startsWith("application/json")
    ) {
      return apiError(
        415,
        "unsupported_media_type",
        "Content-Type must be application/json",
      );
    }
    const payload = await readCallbackBody(request, 1024 * 1024);
    if (payload instanceof Response) return payload;
    try {
      JSON.parse(payload);
    } catch {
      return apiError(400, "invalid_request", "Event batch must be valid JSON");
    }
    const token =
      request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const result = await containerStub(env, callbackMatch[1]!).enqueueEvents(
      token,
      payload,
    );
    return result.accepted
      ? json({ accepted: true }, 202)
      : apiError(
          result.status ?? 401,
          result.status ? "history_rejected" : "unauthorized",
          result.message ?? "A valid callback token is required",
        );
  }
  const interactionMatch = /^\/internal\/v1\/jobs\/([^/]+)\/interactions$/.exec(
    url.pathname,
  );
  if (
    interactionMatch &&
    request.method === "POST" &&
    isUuid(interactionMatch[1]!)
  ) {
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .startsWith("application/json")
    ) {
      return apiError(
        415,
        "unsupported_media_type",
        "Content-Type must be application/json",
      );
    }
    const maximum =
      (appConfig.interactions.provider === "slack"
        ? appConfig.interactions.maxRequestBytes
        : 65_536) + 1024;
    const payload = await readCallbackBody(request, maximum);
    if (payload instanceof Response) return payload;
    const token =
      request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const interactionStub = containerStub(
      env,
      interactionMatch[1]!,
    ) as unknown as {
      requestInteraction(
        token: string,
        payload: string,
      ): Promise<InteractionCallbackResult>;
    };
    const result = await interactionStub.requestInteraction(token, payload);
    return result.accepted
      ? json(result, result.state === "pending" ? 202 : 200)
      : apiError(
          result.status ?? 400,
          result.code ?? "invalid_request",
          result.message ?? "Interaction request was rejected",
        );
  }
  const budgetMatch = /^\/internal\/v1\/jobs\/([^/]+)\/token-usage$/.exec(
    url.pathname,
  );
  if (budgetMatch && request.method === "POST" && isUuid(budgetMatch[1]!)) {
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .startsWith("application/json")
    ) {
      return apiError(
        415,
        "unsupported_media_type",
        "Content-Type must be application/json",
      );
    }
    const payload = await readCallbackBody(request, 8192);
    if (payload instanceof Response) return payload;
    const token =
      request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
    const stub = containerStub(env, budgetMatch[1]!) as unknown as {
      recordTokenUsage(
        token: string,
        payload: string,
      ): Promise<TokenBudgetCallbackResult>;
    };
    const result = await stub.recordTokenUsage(token, payload);
    return result.accepted
      ? json(result)
      : apiError(
          result.status ?? 400,
          result.code ?? "invalid_request",
          result.message ?? "Token usage request was rejected",
        );
  }
  if (!url.pathname.startsWith("/v1/"))
    return apiError(404, "not_found", "Not found");
  if (
    !(await isAuthorized(request, env as unknown as Record<string, unknown>))
  ) {
    return apiError(401, "unauthorized", "A valid bearer token is required", {
      request_id: crypto.randomUUID(),
    });
  }

  const resource =
    /^\/v1\/jobs\/([^/]+)\/(events|traces|metrics|artifacts(?:\/.*)?)$/.exec(
      url.pathname,
    );
  if (resource && isUuid(resource[1]!)) {
    if (request.method !== "GET")
      return apiError(
        405,
        "method_not_allowed",
        "Use GET",
        {},
        { Allow: "GET" },
      );
    const stub = containerStub(env, resource[1]!);
    const job = await stub.getJob();
    if (!job) return apiError(404, "not_found", "Job not found");
    if (resource[2]!.startsWith("artifacts/")) {
      let artifactPath: string;
      try {
        artifactPath = decodeURIComponent(resource[2]!.slice(10));
      } catch {
        return apiError(400, "invalid_path", "Invalid artifact path");
      }
      if (
        !artifactPath ||
        artifactPath
          .split("/")
          .some((part) => !part || part === "." || part === "..") ||
        artifactPath.includes("\\")
      )
        return apiError(400, "invalid_path", "Invalid artifact path");
      const artifact = job.result?.artifacts.find(
        (item) => item.path === artifactPath,
      );
      if (!artifact || !artifact.key.startsWith(`jobs/${job.job_id}/`))
        return apiError(404, "not_found", "Artifact not found");
      const object = await env.RUNNER_STORAGE.get(artifact.key);
      if (!object) return apiError(404, "not_found", "Artifact not found");
      return new Response(object.body, {
        headers: {
          "content-type": artifact.content_type,
          "content-length": String(artifact.size),
          "cache-control": "no-store",
          "content-disposition": `attachment; filename*=UTF-8''${encodeURIComponent(artifactPath.split("/").at(-1)!)}`,
        },
      });
    }
    const limit = Number(url.searchParams.get("limit") ?? 100);
    if (!Number.isInteger(limit) || limit < 1 || limit > 200)
      return apiError(400, "invalid_limit", "limit must be between 1 and 200");
    try {
      return json(
        await stub.getHistory(
          resource[2] as "events" | "traces" | "metrics",
          limit,
          url.searchParams.get("cursor"),
        ),
      );
    } catch (error) {
      if (error instanceof HistoryError)
        return apiError(error.status, "invalid_cursor", error.message);
      throw error;
    }
  }
  if (url.pathname === "/v1/jobs" && request.method === "GET") {
    const limit = Number(url.searchParams.get("limit") ?? 100);
    if (!Number.isInteger(limit) || limit < 1 || limit > 200)
      return apiError(400, "invalid_limit", "limit must be between 1 and 200");
    const coordinator = env.JOB_COORDINATOR.get(
      env.JOB_COORDINATOR.idFromName("global"),
    );
    let page;
    try {
      page = await coordinator.listJobs(limit, url.searchParams.get("cursor"));
    } catch {
      return apiError(400, "invalid_cursor", "Invalid cursor");
    }
    const jobs = await Promise.all(
      page.items.map((id) => containerStub(env, id).getJob()),
    );
    return json({
      items: jobs.filter((job) => job !== null),
      next_cursor: page.next_cursor,
    });
  }

  if (url.pathname === "/v1/workflow-budget/reconcile") {
    if (request.method !== "POST")
      return apiError(
        405,
        "method_not_allowed",
        "Use POST /v1/workflow-budget/reconcile",
        {},
        { Allow: "POST" },
      );
    try {
      const bundle = await loadActiveWorkflowBundle(env);
      const workflow = bundle.manifest.workflow;
      const budget = env.WORKFLOW_BUDGET.get(
        env.WORKFLOW_BUDGET.idFromName(workflow.name),
      );
      const availability = await budget.configure({
        digest: bundle.manifest.digest,
        sortKey: bundle.manifest.sort_key,
        policy: workflow.token_budget ?? null,
      });
      const scheduler = env.SCHEDULE_COORDINATOR.get(
        env.SCHEDULE_COORDINATOR.idFromName("global"),
      );
      const scheduled = await scheduler.wakeBudgetDeferred(workflow.name);
      return json({
        workflow: workflow.name,
        enabled: Boolean(workflow.token_budget),
        allowed: availability.allowed,
        used: availability.used,
        limit: availability.limit,
        reset_at:
          availability.resetAt === null
            ? null
            : new Date(availability.resetAt).toISOString(),
        scheduled_occurrences_woken: scheduled,
      });
    } catch (error) {
      return apiError(
        503,
        "workflow_budget_reconcile_failed",
        error instanceof Error
          ? error.message
          : "Workflow budget reconciliation failed",
        {},
        { "retry-after": "30" },
      );
    }
  }

  if (url.pathname === "/v1/preflight") {
    if (request.method !== "GET") {
      return apiError(
        405,
        "method_not_allowed",
        "Use GET /v1/preflight",
        {},
        { Allow: "GET" },
      );
    }
    return preflightResponse(env);
  }

  if (url.pathname === "/v1/jobs") {
    if (request.method !== "POST")
      return apiError(
        405,
        "method_not_allowed",
        "Use POST /v1/jobs",
        {},
        { Allow: "POST" },
      );
    if (!appConfig.runnerUrl)
      return apiError(
        503,
        "callback_unconfigured",
        "RUNNER_URL must identify the public callback origin",
      );
    const parsed = await readCreateJobBody(request);
    if (parsed instanceof Response) return parsed;
    let bundle;
    try {
      bundle = await loadActiveWorkflowBundle(env);
    } catch (error) {
      return apiError(
        503,
        "workflow_unavailable",
        error instanceof Error
          ? error.message
          : "Active workflow is unavailable",
        {},
        { "retry-after": "30" },
      );
    }

    const idempotencyKey = request.headers.get("idempotency-key");
    if (
      idempotencyKey &&
      (idempotencyKey.length > 128 || idempotencyKey.trim().length === 0)
    ) {
      return apiError(
        400,
        "invalid_idempotency_key",
        "Idempotency-Key must contain 1 to 128 characters",
      );
    }
    const requestHash = await sha256(bundle.manifest.digest);
    const keyHash = idempotencyKey ? await sha256(idempotencyKey) : null;
    const jobId = crypto.randomUUID();
    const createdAt = Date.now();
    setSpanAttributes(span, {
      "agent_runner.job.id": jobId,
      "agent_runner.workflow.name": bundle.manifest.workflow.name,
      "agent_runner.workflow.version": bundle.manifest.digest,
    });
    const submission = await submitWorkflowJob(env, {
      jobId,
      createdAt,
      bundle,
      keyHash,
      requestHash,
      callbackBaseURL: appConfig.runnerUrl!,
      trigger: { type: "api" },
    });

    if (submission.kind === "full") {
      return apiError(
        503,
        "capacity_unavailable",
        "All runner instances are busy",
        {},
        { "retry-after": "30" },
      );
    }
    if (submission.kind === "budget_exhausted") {
      const resetAt = submission.budget.resetAt ?? Date.now() + 30_000;
      const rejection = tokenBudgetRejection(submission.budget);
      return apiError(
        429,
        "token_budget_exhausted",
        rejection.message,
        { ...rejection.details, reset_at: new Date(resetAt).toISOString() },
        {
          "retry-after": String(
            Math.max(1, Math.ceil((resetAt - Date.now()) / 1000)),
          ),
        },
      );
    }
    if (submission.kind === "conflict") {
      return apiError(
        409,
        "idempotency_conflict",
        "Idempotency-Key was already used with a different workflow version",
      );
    }
    if (submission.kind === "missing") {
      return apiError(
        409,
        "idempotency_state_missing",
        "The idempotent job is no longer available",
      );
    }
    if (submission.kind === "existing") {
      setFlowTraceAttributes(span, submission.jobId);
      const existing = submission.job;
      return json(
        {
          job_id: submission.jobId,
          workflow: existing.workflow,
          workflow_version: existing.workflow_version,
          trigger: existing.trigger,
          status: existing.status,
          created_at: existing.created_at,
          status_url: jobPath(submission.jobId),
        },
        202,
      );
    }
    if (submission.kind === "accepted") {
      const job = submission.job;
      setFlowTraceAttributes(span, submission.jobId, submission.traceId);
      return json(
        {
          job_id: submission.jobId,
          workflow: job.workflow,
          workflow_version: job.workflow_version,
          trigger: job.trigger,
          status: job.status,
          created_at: job.created_at,
          status_url: jobPath(submission.jobId),
        },
        202,
      );
    }
    const message =
      submission.error instanceof Error
        ? submission.error.message
        : "Container failed to start";
    return apiError(503, "container_start_failed", message, {
      job_id: submission.jobId,
      status_url: jobPath(submission.jobId),
    });
  }

  const match = /^\/v1\/jobs\/([^/]+)$/.exec(url.pathname);
  if (!match || !isUuid(match[1]!))
    return apiError(404, "job_not_found", "Job not found");
  const jobId = match[1]!;
  const stub = containerStub(env, jobId);

  if (request.method === "GET") {
    const job = await stub.getJob();
    return job ? json(job) : apiError(404, "job_not_found", "Job not found");
  }
  if (request.method === "DELETE") {
    const before = await stub.getJob();
    if (!before) return apiError(404, "job_not_found", "Job not found");
    if (
      [
        "succeeded",
        "partial",
        "failed",
        "timed_out",
        "cancelled",
        "interrupted",
      ].includes(before.status)
    ) {
      return apiError(
        409,
        "job_already_finished",
        `Job is already ${before.status}`,
        { job: before },
      );
    }
    const job = (await stub.cancelJob()) as JobView;
    return json(job, before.status === "cancelled" ? 200 : 202);
  }
  return apiError(
    405,
    "method_not_allowed",
    "Use GET or DELETE for a job",
    {},
    { Allow: "GET, DELETE" },
  );
}

/** Traces one request with normalized routing and delegates to the stable HTTP API. */
export function handleRequest(
  request: Request,
  env: Env,
  tracer: Tracer = noOpTracer,
): Promise<Response> {
  const operation = requestTrace(request);
  return traceAsync(
    tracer,
    operation.name,
    {
      "http.request.method": request.method,
      "http.route": operation.route,
      "agent_runner.job.id": operation.jobId,
      "flow.trace": operation.jobId ? true : undefined,
      "flow.run.id": operation.jobId,
    },
    async (span) => {
      const response = await dispatchRequest(request, env, span);
      recordHttpResult(span, response);
      return response;
    },
  );
}
