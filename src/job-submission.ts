import type { Env, JobTrigger, JobView, WorkflowBundle } from "./types.ts";
import type { BudgetAvailability } from "./workflow-budget.ts";
import { newFlowTraceContext } from "./tracing.ts";

function containerStub(env: Env, jobId: string) {
  return env.AGENT_CONTAINER.get(env.AGENT_CONTAINER.idFromName(jobId));
}

function coordinatorStub(env: Env) {
  return env.JOB_COORDINATOR.get(env.JOB_COORDINATOR.idFromName("global"));
}

export type SubmissionResult =
  | { kind: "accepted"; jobId: string; job: JobView; traceId: string }
  | { kind: "existing"; jobId: string; job: JobView }
  | { kind: "full"; jobId: string }
  | { kind: "budget_exhausted"; jobId: string; budget: BudgetAvailability }
  | { kind: "conflict"; jobId: string }
  | { kind: "missing"; jobId: string }
  | {
      kind: "start_failed";
      jobId: string;
      authoritative: boolean;
      error: unknown;
    };

interface SubmissionInput {
  bundle: WorkflowBundle;
  keyHash: string | null;
  requestHash: string;
  callbackBaseURL: string;
  trigger: JobTrigger;
  jobId?: string;
  createdAt?: number;
  resumeExisting?: boolean;
}

/** Admits and starts one workflow job while preserving idempotency across transports. */
export async function submitWorkflowJob(
  env: Env,
  input: SubmissionInput,
): Promise<SubmissionResult> {
  const jobId = input.jobId ?? crypto.randomUUID();
  const container = containerStub(env, jobId);
  if (input.resumeExisting && input.jobId) {
    const resumed = await container.getJob();
    if (resumed) return { kind: "existing", jobId, job: resumed };
  }
  const createdAt = input.createdAt ?? Date.now();
  const coordinator = coordinatorStub(env);
  const admission = await coordinator.acquire(
    jobId,
    input.keyHash,
    input.requestHash,
    createdAt,
    input.bundle.manifest.workflow.workflow_timeout_ms,
  );
  if (admission.kind === "full") return { kind: "full", jobId };
  if (admission.kind === "existing") {
    if (!admission.requestMatches)
      return { kind: "conflict", jobId: admission.jobId ?? jobId };
    const existingId = admission.jobId!;
    const existing = await containerStub(env, existingId).getJob();
    return existing
      ? { kind: "existing", jobId: existingId, job: existing }
      : { kind: "missing", jobId: existingId };
  }

  const workflow = input.bundle.manifest.workflow;
  if (workflow.token_budget) {
    const budget = env.WORKFLOW_BUDGET.get(
      env.WORKFLOW_BUDGET.idFromName(workflow.name),
    );
    const availability = await budget.availability(
      {
        digest: input.bundle.manifest.digest,
        sortKey: input.bundle.manifest.sort_key,
        policy: workflow.token_budget,
      },
      createdAt,
    );
    if (!availability.allowed) {
      await coordinator.rejectAdmission(jobId, input.keyHash);
      if (availability.reason === "suspended_jobs") {
        await budget.configure(
          {
            digest: input.bundle.manifest.digest,
            sortKey: input.bundle.manifest.sort_key,
            policy: workflow.token_budget,
          },
          createdAt,
        );
      }
      return { kind: "budget_exhausted", jobId, budget: availability };
    }
  }

  try {
    const flowTrace = newFlowTraceContext();
    const job = await container.startJob({
      jobId,
      traceId: flowTrace.traceId,
      rootSpanId: flowTrace.rootSpanId,
      bundle: input.bundle,
      requestHash: input.requestHash,
      createdAt,
      callbackBaseURL: input.callbackBaseURL,
      trigger: input.trigger,
    });
    return { kind: "accepted", jobId, job, traceId: flowTrace.traceId };
  } catch (error) {
    let authoritative: boolean | null = null;
    try {
      authoritative = Boolean(await container.getJob());
    } catch {
      /* scheduled retries keep the lease and reuse the same job ID after an ambiguous RPC */
    }
    if (
      authoritative !== true &&
      (!input.resumeExisting || authoritative === false)
    ) {
      await coordinator.release(jobId);
    }
    return {
      kind: "start_failed",
      jobId,
      authoritative: authoritative === true,
      error,
    };
  }
}
