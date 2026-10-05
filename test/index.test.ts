vi.mock("../src/generated-config.ts",async(importOriginal)=>{const original=await importOriginal<typeof import("../src/generated-config.ts")>();return {...original,appConfig:{...original.appConfig,runnerUrl:"https://runner.test"}};});
import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";
import { preflightResponse, handleRequest } from "../src/api.ts";
import { appConfig } from "../src/generated-config.ts";
import { ACTIVE_JOB_STATUSES, type Env, type JobView, type StartJobInput, type WorkflowBundleManifest } from "../src/types.ts";
import { isWorkflowBundleManifest, loadActiveWorkflowBundle, loadWorkflowBundleArchive, workflowBundleTelemetry } from "../src/workflow.ts";
import { containerSleepAfter, workflowDeadlineAt, workflowDeadlineElapsed } from "../src/timeouts.ts";
import type { TraceSpan, Tracer } from "../src/tracing.ts";
import type { BudgetAvailability } from "../src/workflow-budget.ts";

const token = "r".repeat(48);
const jobId = "123e4567-e89b-42d3-a456-426614174000";
const bundleDigest = "a".repeat(64);
const prompt = "Run the configured task.";
const promptDigest = createHash("sha256").update(prompt).digest("hex");
const archiveContent = "compressed archive placeholder";
const archiveDigest = createHash("sha256").update(archiveContent).digest("hex");
const sortKey = "20260903T214512.347Z";
const archiveKey = `bundles/${sortKey}-${bundleDigest.slice(0, 12)}/bundle.tgz`;
const manifest: WorkflowBundleManifest = {
  version: 1, digest: bundleDigest, sort_key: sortKey,
  workflow: { version: 1, name: "default", workflow_timeout_ms: 3600000, default_step_timeout_ms: 900000, default_harness: "opencode", default_model: "claude-sonnet-4-6", default_reasoning_effort: "medium", steps: [
    { id: "check", command: ["git", "--version"], timeout_ms: 30000 },
    { id: "run", prompt: "foo.md", allow_user_input: false, harness: "opencode", provider: "anthropic", model: "claude-sonnet-4-6", reasoning_effort: "medium", required_metrics: [], timeout_ms: 900000 },
  ] },
  archive: { key: archiveKey, size: new TextEncoder().encode(archiveContent).byteLength, sha256: archiveDigest },
  files: [{ kind: "prompt", path: "foo.md", size: new TextEncoder().encode(prompt).byteLength, sha256: promptDigest, executable: false }],
  total_bytes: new TextEncoder().encode(prompt).byteLength,
};
const job: JobView = {
  job_id: jobId, workflow: "default", workflow_version: bundleDigest, current_step: "run", status: "running",
  trigger: { type: "api" },
  created_at: "2026-08-29T12:00:00.000Z", started_at: "2026-08-29T12:00:01.000Z", finished_at: null,
  expires_at: "2026-08-30T12:00:00.000Z",
  history: { state: "pending", truncated:false,dropped_events:0,dropped_traces:0,pending_records:1,retained_records:1, last_error: null, updated_at: "2026-08-29T12:00:01.000Z" },
};

function namespace(stub: object) {
  return { idFromName: vi.fn((name: string) => name), idFromString: vi.fn((id: string) => id), get: vi.fn(() => stub) };
}

function r2Object(text: string) {
  return { size: new TextEncoder().encode(text).byteLength, text: async () => text, arrayBuffer: async () => new TextEncoder().encode(text).buffer };
}

function makeEnv(admission: object = { kind: "acquired", jobId }, active = true, activeManifest: unknown = manifest) {
  const container = {
    startJob: vi.fn(async (_input: StartJobInput) => job), getJob: vi.fn(async () => job),
    cancelJob: vi.fn(async () => ({ ...job, status: "cancelling" })), enqueueEvents: vi.fn(async () => ({ accepted: true })),
    getHistory: vi.fn(async()=>({items:[],next_cursor:null})),
    recordTokenUsage: vi.fn(async () => ({ accepted: true, action: "continue" })),
  };
  const coordinator = {
    acquire: vi.fn(async (_jobId: string, _keyHash: string | null, _requestHash: string, _now: number, _workflowTimeoutMs: number) => admission),
    registerJob: vi.fn(async()=>undefined),
    release: vi.fn(async () => undefined),
    listJobs: vi.fn(async()=>({items:[jobId],next_cursor:null})),
    rejectAdmission: vi.fn(async () => undefined),
  };
  const budget = {
    availability: vi.fn(async (): Promise<BudgetAvailability> => ({ allowed: true, limit: 100, used: 0, resetAt: Date.UTC(2026, 8, 4) })),
    configure: vi.fn(async () => ({ allowed: true, limit: 100, used: 0, resetAt: Date.UTC(2026, 8, 4) })),
  };
  const scheduler = { wakeBudgetDeferred: vi.fn(async () => 0) };
  const storage = {
    put: vi.fn(async()=>undefined),delete:vi.fn(async()=>undefined),
    get: vi.fn(async (key: string) => key.endsWith("active.json") ? (active ? r2Object(JSON.stringify(activeManifest)) : null) : key === archiveKey ? r2Object(archiveContent) : key.startsWith("preflight/")?r2Object("ready"):null),
  };
  return { env: {
    AGENT_CONTAINER: namespace(container), JOB_COORDINATOR: namespace(coordinator), WORKFLOW_BUDGET: namespace(budget),
    SCHEDULE_COORDINATOR: namespace(scheduler), RUNNER_STORAGE: storage, RUNNER_API_TOKEN: token, OPENAI_API_KEY: "test-key", ANTHROPIC_API_KEY:"test-key",
  } as unknown as Env, container, coordinator, budget, scheduler, storage };
}

function apiRequest(path: string, init: RequestInit = {}) {
  const headers = new Headers(init.headers); headers.set("authorization", `Bearer ${token}`);
  return new Request(`https://runner.test${path}`, { ...init, headers });
}

describe("job API", () => {
  it("validates deployed storage and only selected provider secrets",async()=>{
    const {env,storage}=makeEnv();
    const response=await preflightResponse(env);
    expect(response.status).toBe(200);
    expect(storage.put).toHaveBeenCalled();expect(storage.delete).toHaveBeenCalled();
    const missing=await preflightResponse({...env,ANTHROPIC_API_KEY:""} as unknown as Env);
    expect(missing.status).toBe(503);
    expect(JSON.stringify(await missing.json())).toContain("ANTHROPIC_API_KEY");
  });
  it("lists retained jobs and routes bounded history pages",async()=>{
    const {env,container}=makeEnv();
    const list=await handleRequest(apiRequest("/v1/jobs?limit=20"),env);
    expect(list.status).toBe(200);expect((await list.json() as any).items[0].job_id).toBe(jobId);
    const page=await handleRequest(apiRequest(`/v1/jobs/${jobId}/events?limit=20&cursor=next`),env);
    expect(page.status).toBe(200);expect(container.getHistory).toHaveBeenCalledWith("events",20,"next");
    expect((await handleRequest(apiRequest(`/v1/jobs/${jobId}/metrics?limit=201`),env)).status).toBe(400);
  });
  it("rejects artifact traversal and unrecorded downloads",async()=>{
    const {env}=makeEnv();
    expect((await handleRequest(apiRequest(`/v1/jobs/${jobId}/artifacts/%2E%2E%2Fsecret`),env)).status).toBe(400);
    expect((await handleRequest(apiRequest(`/v1/jobs/${jobId}/artifacts/missing.txt`),env)).status).toBe(404);
  });

  it("exposes sleeping as a distinct active job state", () => {
    expect(ACTIVE_JOB_STATUSES).toContain("sleeping");
    expect(ACTIVE_JOB_STATUSES).toContain("budget_suspended");
  });

  it("traces normalized API operations without recording raw paths", async () => {
    const { env } = makeEnv();
    const spans: Array<{ name: string; attributes: Record<string, string | number | boolean> }> = [];
    const tracer: Tracer = {
      enterSpan: (name, callback, ...args) => {
        const attributes: Record<string, string | number | boolean> = {};
        const span: TraceSpan = {
          isTraced: true,
          setAttribute(key, value) {
            attributes[key] = value;
            return this;
          },
        };
        spans.push({ name, attributes });
        return callback(span, ...args);
      },
    };

    const response = await handleRequest(apiRequest(`/v1/jobs/${jobId}`), env, tracer);
    expect(response.status).toBe(200);
    expect(spans).toEqual([{
      name: "agent_runner.job.get",
      attributes: {
        "http.request.method": "GET",
        "http.route": "/v1/jobs/:job_id",
        "agent_runner.job.id": jobId,
        "flow.trace": true,
        "flow.run.id": jobId,
        "http.response.status_code": 200,
        "agent_runner.outcome": "success",
      },
    }]);
  });

  it("derives and enforces the one-hour aggregate workflow deadline", () => {
    const startedAt = Date.UTC(2026, 7, 29, 12);
    const deadlineAt = workflowDeadlineAt(startedAt, 60 * 60 * 1000);
    expect(deadlineAt).toBe(startedAt + 60 * 60 * 1000);
    expect(workflowDeadlineElapsed(deadlineAt, deadlineAt - 1)).toBe(false);
    expect(workflowDeadlineElapsed(deadlineAt, deadlineAt)).toBe(true);
    expect(containerSleepAfter(60 * 60 * 1000)).toBe("62m");
  });

  it("validates mixed command and LLM workflow manifests", () => {
    expect(isWorkflowBundleManifest(manifest)).toBe(true);
    const metricStep = { ...manifest.workflow.steps[1]!, required_metrics: [
      { namespace: "haiku", key: "num_lines", description: "number of lines" },
    ] };
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, metricStep] } })).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, { ...metricStep, required_metrics: ["haiku.num_lines"] }] } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, { ...metricStep, required_metrics: [{ namespace: "haiku", key: "num_lines", description: "" }] }] } })).toBe(false);
    const { allow_user_input: _allowUserInput, ...legacyAgentStep } = manifest.workflow.steps[1] as Extract<(typeof manifest.workflow.steps)[number], { prompt: string }>;
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, legacyAgentStep] } })).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, { ...manifest.workflow.steps[1]!, allow_user_input: true }] } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [manifest.workflow.steps[0]!, { ...manifest.workflow.steps[1]!, harness: "pi", allow_user_input: true }] } })).toBe(false);
    expect(isWorkflowBundleManifest({...manifest,files:[],total_bytes:0,workflow:{...manifest.workflow,steps:[{id:"check",command:["true"],timeout_ms:1000}]}})).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [{ id: "check", command: ["true"], timeout_ms: 1000 }] } })).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [{ id: "check", command: [""], timeout_ms: 1000 }, manifest.workflow.steps[1]!] } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [{ ...manifest.workflow.steps[1]!, reasoning_effort: "extreme" }] } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, steps: [{ id: "check", command: ["true"], reasoning_effort: "low", timeout_ms: 1000 }, manifest.workflow.steps[1]!] } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, default_harness: "unknown" } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, default_model: "unknown" } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, default_reasoning_effort: "extreme" } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, memory_enabled: "yes" } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, memory_enabled: true } })).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, token_budget: { limit: 100_000, period: "week" } } })).toBe(true);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, token_budget: { limit: 0, period: "day" } } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, workflow: { ...manifest.workflow, workflow_timeout_ms: 1000, default_step_timeout_ms: 2000 } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, archive: { ...manifest.archive, key: "wrong" } })).toBe(false);
    expect(isWorkflowBundleManifest({ ...manifest, archive: { ...manifest.archive, key: `deployments/runner/${manifest.archive.key}` } })).toBe(false);

    const { default_harness: _harness, default_model: _model, default_reasoning_effort: _effort, ...legacyWorkflow } = manifest.workflow;
    expect(isWorkflowBundleManifest({ ...manifest, workflow: legacyWorkflow })).toBe(true);
  });

  it("summarizes materialized workflow bundles without exposing file paths", () => {
    expect(workflowBundleTelemetry(manifest)).toEqual({
      manifest_version: 1,
      digest: bundleDigest,
      sort_key: sortKey,
      archive: manifest.archive,
      files: {
        count: 1,
        total_bytes: manifest.total_bytes,
        by_kind: {
          prompt: { count: 1, bytes: manifest.files[0]!.size },
          script: { count: 0, bytes: 0 },
          skill: { count: 0, bytes: 0 },
        },
      },
    });
    expect(JSON.stringify(workflowBundleTelemetry(manifest))).not.toContain("foo.md");
  });

  it("rejects legacy active manifests", async () => {
    const legacyManifest = { ...manifest, version: 0 };
    expect(isWorkflowBundleManifest(legacyManifest)).toBe(false);
    const { env } = makeEnv({ kind: "acquired", jobId }, true, legacyManifest);
    await expect(loadActiveWorkflowBundle(env)).rejects.toThrow(/invalid or incompatible/);
  });

  it("loads and verifies the pinned workflow archive", async () => {
    const { env } = makeEnv();
    const bytes = await loadWorkflowBundleArchive(env, manifest);
    expect(new TextDecoder().decode(bytes)).toBe(archiveContent);
  });

  it("accepts authenticated internal event batches", async () => {
    const { env, container } = makeEnv();
    const response = await handleRequest(new Request(`https://runner.test/internal/v1/jobs/${jobId}/events`, { method: "POST", headers: { authorization: "Bearer callback-token", "content-type": "application/json" }, body: JSON.stringify({ schema_version: 3, events: [], spans: [] }) }), env);
    expect(response.status).toBe(202);
    expect(container.enqueueEvents).toHaveBeenCalledWith("callback-token", expect.any(String));
  });

  it("forwards bounded token usage callbacks to the owning job object", async () => {
    const { env, container } = makeEnv();
    const payload = JSON.stringify({ step_id: "run", step_index: 1, tokens: 110, completed_at: new Date().toISOString(), remaining_workflow_ms: 1000 });
    const response = await handleRequest(new Request(`https://runner.test/internal/v1/jobs/${jobId}/token-usage`, {
      method: "POST", headers: { authorization: "Bearer callback-token", "content-type": "application/json" }, body: payload,
    }), env);
    expect(response.status).toBe(200);
    expect(container.recordTokenUsage).toHaveBeenCalledWith("callback-token", payload);
  });

  it("rejects unauthenticated calls", async () => {
    const { env } = makeEnv();
    expect((await handleRequest(new Request("https://runner.test/v1/jobs"), env)).status).toBe(401);
  });



  it("protects the preflight route and restricts it to GET", async () => {
    const { env } = makeEnv();
    expect((await handleRequest(new Request("https://runner.test/v1/preflight"), env)).status).toBe(401);
    const response = await handleRequest(apiRequest("/v1/preflight", { method: "POST" }), env);
    expect(response.status).toBe(405);
    expect(response.headers.get("allow")).toBe("GET");
  });

  it("loads the active bundle and creates a workflow job", async () => {
    const { env, container, coordinator, storage } = makeEnv();
    vi.spyOn(crypto, "randomUUID").mockReturnValueOnce(jobId);
    const response = await handleRequest(apiRequest("/v1/jobs", { method: "POST", headers: { "idempotency-key": "request-1" } }), env);
    expect(response.status).toBe(202);
    expect(await response.json()).toEqual(expect.objectContaining({ job_id: jobId, workflow: "default", workflow_version: bundleDigest, trigger: { type: "api" } }));
    expect(coordinator.acquire).toHaveBeenCalledOnce();
    expect(coordinator.acquire.mock.calls[0]![4]).toBe(3600000);
    expect(container.startJob).toHaveBeenCalledWith(expect.objectContaining({ bundle: expect.objectContaining({ manifest }) }));
    expect(container.startJob).toHaveBeenCalledWith(expect.objectContaining({ trigger: { type: "api" } }));
    expect(container.startJob.mock.calls[0]![0].traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(container.startJob.mock.calls[0]![0].rootSpanId).toMatch(/^[0-9a-f]{16}$/);
    expect(storage.get).toHaveBeenCalledWith("active.json");
  });

  it("reconciles the active workflow budget and wakes deferred schedules", async () => {
    const budgeted = { ...manifest, workflow: { ...manifest.workflow, token_budget: { limit: 100, period: "day" as const } } };
    const { env, budget, scheduler } = makeEnv({ kind: "acquired", jobId }, true, budgeted);
    const response = await handleRequest(apiRequest("/v1/workflow-budget/reconcile", { method: "POST" }), env);
    expect(response.status).toBe(200);
    expect(budget.configure).toHaveBeenCalledWith({ digest: bundleDigest, sortKey, policy: budgeted.workflow.token_budget });
    expect(scheduler.wakeBudgetDeferred).toHaveBeenCalledWith("default");
  });

  it("rejects new work after rolling back capacity when the workflow budget is exhausted", async () => {
    const budgeted = { ...manifest, workflow: { ...manifest.workflow, token_budget: { limit: 100, period: "day" as const } } };
    const { env, budget, coordinator, container } = makeEnv({ kind: "acquired", jobId }, true, budgeted);
    budget.availability.mockResolvedValueOnce({ allowed: false, limit: 100, used: 125, resetAt: Date.now() + 60_000, reason: "exhausted" });
    vi.spyOn(crypto, "randomUUID").mockReturnValueOnce(jobId);
    const response = await handleRequest(apiRequest("/v1/jobs", { method: "POST" }), env);
    expect(response.status).toBe(429);
    expect(await response.json()).toEqual(expect.objectContaining({
      error: expect.objectContaining({ message: "The workflow token budget is exhausted by 25 tokens (125 used of 100)" }),
      used_tokens: 125,
      limit_tokens: 100,
      overage_tokens: 25,
    }));
    expect(coordinator.rejectAdmission).toHaveBeenCalledWith(jobId, null);
    expect(container.startJob).not.toHaveBeenCalled();
  });

  it("fails before capacity acquisition when no bundle is active", async () => {
    const { env, coordinator } = makeEnv({ kind: "acquired", jobId }, false);
    const response = await handleRequest(apiRequest("/v1/jobs", { method: "POST" }), env);
    expect(response.status).toBe(503);
    expect(((await response.json()) as any).error.code).toBe("workflow_unavailable");
    expect(coordinator.acquire).not.toHaveBeenCalled();
  });

  it("returns an existing idempotent job", async () => {
    const { env, container } = makeEnv({ kind: "existing", jobId, requestMatches: true });
    const response = await handleRequest(apiRequest("/v1/jobs", { method: "POST", headers: { "idempotency-key": "request-1" } }), env);
    expect(response.status).toBe(202);
    expect(((await response.json()) as any).workflow_version).toBe(bundleDigest);
    expect(container.startJob).not.toHaveBeenCalled();
  });

  it("returns a retryable capacity response", async () => {
    const { env } = makeEnv({ kind: "full" });
    const response = await handleRequest(apiRequest("/v1/jobs", { method: "POST" }), env);
    expect(response.status).toBe(503);
    expect(response.headers.get("retry-after")).toBe("30");
  });

  it("gets and cancels a running job", async () => {
    const { env, container } = makeEnv();
    expect((await handleRequest(apiRequest(`/v1/jobs/${jobId}`), env)).status).toBe(200);
    expect((await handleRequest(apiRequest(`/v1/jobs/${jobId}`, { method: "DELETE" }), env)).status).toBe(202);
    expect(container.cancelJob).toHaveBeenCalledOnce();
  });
});
