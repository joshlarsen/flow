import { createHash } from "node:crypto";
import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("cloudflare:workers", () => ({
  DurableObject: class<Env> {
    protected ctx: any;
    protected env: Env;
    constructor(ctx: any, env: Env) { this.ctx = ctx; this.env = env; }
  },
  tracing: {
    enterSpan: (_name: string, callback: (...args: any[]) => unknown, ...args: unknown[]) => callback({
      isTraced: false,
      setAttribute() { return this; },
    }, ...args),
  },
}));

import { appConfig } from "../src/generated-config.ts";
import { ScheduleCoordinator } from "../src/scheduler.ts";
import type { Env, JobView, WorkflowBundleManifest } from "../src/types.ts";

class SqlStorage {
  constructor(readonly database: DatabaseSync) {}
  exec<T>(sql: string, ...params: SQLInputValue[]) {
    if (params.length === 0 && !/^\s*(?:SELECT|INSERT|UPDATE|DELETE)/i.test(sql)) {
      this.database.exec(sql);
      return { toArray: () => [] as T[], one: () => undefined as T | undefined };
    }
    const statement = this.database.prepare(sql);
    if (/^\s*SELECT/i.test(sql)) {
      const rows = statement.all(...params) as T[];
      return { toArray: () => rows, one: () => rows[0] };
    }
    statement.run(...params);
    return { toArray: () => [] as T[], one: () => undefined as T | undefined };
  }
}

const now = Date.UTC(2026, 8, 14, 13);
const workflowDigest = "a".repeat(64);
const prompt = "Run the configured task.";
const promptDigest = createHash("sha256").update(prompt).digest("hex");
const archiveContent = "compressed archive placeholder";
const archiveDigest = createHash("sha256").update(archiveContent).digest("hex");
const sortKey = "20260914T125959.000Z";
const archiveKey = `bundles/${sortKey}-${workflowDigest.slice(0, 12)}/bundle.tgz`;
const manifestKey = archiveKey.replace(/bundle\.tgz$/, "manifest.json");
const manifest: WorkflowBundleManifest = {
  version: 1,
  digest: workflowDigest,
  sort_key: sortKey,
  workflow: {
    version: 1,
    name: "default",
    workflow_timeout_ms: 3_600_000,
    default_step_timeout_ms: 900_000,
    default_harness: "pi",
    default_model: "gpt-5.6-luna",
    default_reasoning_effort: "medium",
    steps: [{ id: "run", prompt: "run.md", allow_user_input: false, harness: "pi", provider: "openai", model: "gpt-5.6-luna", reasoning_effort: "medium", required_metrics: [], timeout_ms: 900_000 }],
  },
  archive: { key: archiveKey, size: archiveContent.length, sha256: archiveDigest },
  files: [{ kind: "prompt", path: "run.md", size: prompt.length, sha256: promptDigest, executable: false }],
  total_bytes: prompt.length,
};

const scheduledJob: JobView = {
  job_id: "123e4567-e89b-42d3-a456-426614174000",
  workflow: "default",
  workflow_version: workflowDigest,
  trigger: { type: "cron", schedule_id: "daily", cron: "0 13 * * *", scheduled_at: new Date(now).toISOString() },
  status: "running",
  current_step: null,
  created_at: new Date(now).toISOString(),
  started_at: new Date(now).toISOString(),
  finished_at: null,
  expires_at: new Date(now + 86_400_000).toISOString(),
  history: {state:"archived",truncated:false,dropped_events:0,dropped_traces:0,pending_records:0,retained_records:0,last_error:null,updated_at:null},
};

function r2Object(value: unknown) {
  const text = JSON.stringify(value);
  return { size: new TextEncoder().encode(text).byteLength, text: async () => text };
}

function namespace(stub: object) {
  return { idFromName: vi.fn((name: string) => name), get: vi.fn(() => stub) };
}

function harness(
  admission: object = { kind: "full" },
  containerOverrides: Record<string, unknown> = {},
  activeManifest: WorkflowBundleManifest = manifest,
  budgetAvailability: object = { allowed: true, limit: 100, used: 0, resetAt: now + 86_400_000 },
) {
  const database = new DatabaseSync(":memory:");
  const alarms: number[] = [];
  const storage = {
    sql: new SqlStorage(database),
    setAlarm: vi.fn(async (time: number) => { alarms.push(time); }),
    deleteAlarm: vi.fn(async () => undefined),
  };
  const container = {
    getJob: vi.fn(async () => null),
    startJob: vi.fn(async () => scheduledJob),
    ...containerOverrides,
  };
  const coordinator = {
    acquire: vi.fn(async () => admission),
    registerJob: vi.fn(async()=>undefined),
    release: vi.fn(async () => undefined),
    rejectAdmission: vi.fn(async () => undefined),
  };
  const budget = {
    availability: vi.fn(async () => budgetAvailability),
    configure: vi.fn(async () => budgetAvailability),
  };
  const env = {
    RUNNER_STORAGE: { get: vi.fn(async (key: string) => key === manifestKey ? r2Object(activeManifest) : null) },
    AGENT_CONTAINER: namespace(container),
    JOB_COORDINATOR: namespace(coordinator),
    WORKFLOW_BUDGET: namespace(budget),
  } as unknown as Env;
  const scheduler = new ScheduleCoordinator({ storage } as unknown as DurableObjectState, env);
  return { scheduler, database, storage, alarms, container, coordinator, budget };
}

function occurrence(scheduledAt = now, id = "daily") {
  return { scheduleId: id, cron: id === "daily" ? "0 13 * * *" : "30 13 * * *", scheduledAt, workflow: "default", workflowDigest, manifestKey };
}

describe("schedule coordinator", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);
    const schedules = appConfig.workflowSchedules as unknown as Array<{ id: string; cron: string }>;
    schedules.splice(0, schedules.length, { id: "daily", cron: "0 13 * * *" }, { id: "afternoon", cron: "30 13 * * *" });
    vi.spyOn(console, "log").mockImplementation(() => undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    (appConfig.workflowSchedules as unknown as unknown[]).splice(0);
  });

  it("deduplicates repeats and replaces only older pending work for one schedule", async () => {
    const { scheduler, database } = harness();
    await scheduler.enqueue(occurrence(now));
    await scheduler.enqueue(occurrence(now - 60_000));
    await scheduler.enqueue(occurrence(now + 60_000));
    await scheduler.enqueue(occurrence(now, "afternoon"));
    const rows = database.prepare("SELECT schedule_id, scheduled_at FROM pending_schedules ORDER BY schedule_id").all();
    expect(rows).toEqual([
      { schedule_id: "afternoon", scheduled_at: now },
      { schedule_id: "daily", scheduled_at: now + 60_000 },
    ]);
    database.close();
  });

  it("retains and defers an occurrence when global capacity is full", async () => {
    const { scheduler, database, coordinator } = harness({ kind: "full" });
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    expect(coordinator.acquire).toHaveBeenCalledOnce();
    expect(database.prepare("SELECT next_attempt_at, attempts FROM pending_schedules").get()).toEqual({
      next_attempt_at: now + 30_000,
      attempts: 0,
    });
    database.close();
  });

  it("makes only budget-deferred occurrences immediately eligible after reconciliation", async () => {
    const { scheduler, database } = harness();
    await scheduler.enqueue(occurrence());
    database.prepare("UPDATE pending_schedules SET next_attempt_at = ?, defer_reason = 'budget' WHERE schedule_id = 'daily'").run(now + 86_400_000);
    expect(await scheduler.wakeBudgetDeferred("default")).toBe(1);
    expect(database.prepare("SELECT next_attempt_at, defer_reason FROM pending_schedules").get()).toEqual({
      next_attempt_at: now,
      defer_reason: null,
    });
    database.close();
  });

  it("retains a scheduled occurrence until its workflow budget is available", async () => {
    const budgeted = { ...manifest, workflow: { ...manifest.workflow, token_budget: { limit: 100, period: "day" as const } } };
    const resetAt = now + 60_000;
    const { scheduler, database, coordinator, container } = harness(
      { kind: "acquired" }, {}, budgeted,
      { allowed: false, limit: 100, used: 100, resetAt, reason: "exhausted" },
    );
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    expect(coordinator.rejectAdmission).toHaveBeenCalledOnce();
    expect(container.startJob).not.toHaveBeenCalled();
    expect(database.prepare("SELECT next_attempt_at, defer_reason FROM pending_schedules").get()).toEqual({
      next_attempt_at: resetAt,
      defer_reason: "budget",
    });
    database.close();
  });

  it("starts and removes admitted work with pinned trigger provenance", async () => {
    const { scheduler, database, container } = harness({ kind: "acquired" });
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    expect(container.startJob).toHaveBeenCalledWith(expect.objectContaining({
      trigger: scheduledJob.trigger,
      bundle: expect.objectContaining({ manifest: expect.objectContaining({ digest: workflowDigest }) }),
    }));
    expect(database.prepare("SELECT COUNT(*) AS count FROM pending_schedules").get()).toEqual({ count: 0 });
    database.close();
  });

  it("does not resubmit after an authoritative container startup failure", async () => {
    const failedJob = { ...scheduledJob, status: "failed" as const };
    const getJob = vi.fn().mockResolvedValueOnce(null).mockResolvedValueOnce(failedJob);
    const startJob = vi.fn(async () => { throw new Error("container boot failed"); });
    const { scheduler, database, coordinator } = harness({ kind: "acquired" }, { getJob, startJob });
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    await scheduler.alarm();
    expect(startJob).toHaveBeenCalledOnce();
    expect(coordinator.release).not.toHaveBeenCalled();
    expect(database.prepare("SELECT COUNT(*) AS count FROM pending_schedules").get()).toEqual({ count: 0 });
    database.close();
  });

  it("retries an ambiguous startup RPC with the same occurrence", async () => {
    const startJob = vi.fn(async () => { throw new Error("RPC disconnected"); });
    const { scheduler, database, coordinator } = harness({ kind: "acquired" }, { startJob });
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    expect(coordinator.release).toHaveBeenCalledOnce();
    expect(database.prepare("SELECT next_attempt_at, attempts FROM pending_schedules").get()).toEqual({
      next_attempt_at: now + 2_000,
      attempts: 1,
    });
    database.close();
  });

  it("retains admission when both startup and reconciliation RPCs are ambiguous", async () => {
    const getJob = vi.fn().mockResolvedValueOnce(null).mockRejectedValueOnce(new Error("RPC disconnected"));
    const startJob = vi.fn(async () => { throw new Error("RPC disconnected"); });
    const { scheduler, database, coordinator } = harness({ kind: "acquired" }, { getJob, startJob });
    await scheduler.enqueue(occurrence());
    await scheduler.alarm();
    expect(coordinator.release).not.toHaveBeenCalled();
    expect(database.prepare("SELECT next_attempt_at, attempts FROM pending_schedules").get()).toEqual({
      next_attempt_at: now + 2_000,
      attempts: 1,
    });
    database.close();
  });

  it("discards pending work when a deployment removes its schedule", async () => {
    const { scheduler, database, coordinator } = harness({ kind: "acquired" });
    await scheduler.enqueue(occurrence());
    const schedules = appConfig.workflowSchedules as unknown as Array<{ id: string; cron: string }>;
    schedules.splice(schedules.findIndex((schedule) => schedule.id === "daily"), 1);
    await scheduler.alarm();
    expect(coordinator.acquire).not.toHaveBeenCalled();
    expect(database.prepare("SELECT COUNT(*) AS count FROM pending_schedules").get()).toEqual({ count: 0 });
    database.close();
  });

  it("expires pending work at the configured retention boundary", async () => {
    const { scheduler, database, coordinator } = harness();
    await scheduler.enqueue(occurrence(now - appConfig.retentionMs));
    await scheduler.alarm();
    expect(coordinator.acquire).not.toHaveBeenCalled();
    expect(database.prepare("SELECT COUNT(*) AS count FROM pending_schedules").get()).toEqual({ count: 0 });
    database.close();
  });
});
