import { DatabaseSync, type SQLInputValue } from "node:sqlite";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("cloudflare:workers", () => ({
  DurableObject: class<Env> {
    protected ctx: any;
    protected env: Env;
    constructor(ctx: any, env: Env) { this.ctx = ctx; this.env = env; }
  },
}));

import { WorkflowBudgetCoordinator, tokenBudgetWindow } from "../src/workflow-budget.ts";
import type { Env } from "../src/types.ts";

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

const day = Date.UTC(2026, 8, 20, 12);
const jobA = "123e4567-e89b-42d3-a456-426614174000";
const jobB = "223e4567-e89b-42d3-a456-426614174001";
const revision = { digest: "a".repeat(64), sortKey: "20260920T120000.000Z", policy: { limit: 100, period: "day" as const } };

function harness(resume: (jobId: string) => Promise<{ state: "accepted" | "deferred" | "gone" }> = async () => ({ state: "accepted" })) {
  const database = new DatabaseSync(":memory:");
  const alarms: number[] = [];
  const storage = {
    sql: new SqlStorage(database),
    setAlarm: vi.fn(async (time: number) => { alarms.push(time); }),
    deleteAlarm: vi.fn(async () => undefined),
  };
  const env = {
    RUNNER_STORAGE: { get: vi.fn(async () => null) },
    AGENT_CONTAINER: {
      idFromName: vi.fn((name: string) => name),
      get: vi.fn((name: string) => ({ resumeBudgetJob: () => resume(name) })),
    },
  } as unknown as Env;
  const coordinator = new WorkflowBudgetCoordinator({ storage, id: { name: "default" } } as unknown as DurableObjectState, env);
  return { coordinator, database, storage, alarms };
}

describe("workflow token budget", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(day);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("uses UTC calendar day and Monday-based week windows", () => {
    expect(tokenBudgetWindow(Date.UTC(2026, 8, 20, 23), "day")).toEqual({
      start: Date.UTC(2026, 8, 20), end: Date.UTC(2026, 8, 21),
    });
    expect(tokenBudgetWindow(Date.UTC(2026, 8, 20, 23), "week")).toEqual({
      start: Date.UTC(2026, 8, 14), end: Date.UTC(2026, 8, 21),
    });
  });

  it("charges completed steps idempotently and resets on the next UTC day", async () => {
    const { coordinator, database } = harness();
    const first = await coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: 60, completedAt: day }, day);
    expect(first).toMatchObject({ allowed: true, used: 60, limit: 100 });
    expect((await coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: 60, completedAt: day }, day)).used).toBe(60);
    await expect(coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: 61, completedAt: day }, day)).rejects.toThrow(/conflicts/);
    expect(await coordinator.charge({ ...revision, jobId: jobB, stepIndex: 0, tokens: 40, completedAt: day }, day)).toMatchObject({
      allowed: false, used: 100, reason: "exhausted", resetAt: Date.UTC(2026, 8, 21),
    });
    expect(await coordinator.availability(revision, Date.UTC(2026, 8, 21))).toMatchObject({ allowed: true, used: 0 });
    database.close();
  });

  it("fails closed for missing usage until the calendar period resets", async () => {
    const { coordinator, database } = harness();
    expect(await coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: null, completedAt: day }, day)).toMatchObject({
      allowed: false, reason: "indeterminate",
    });
    expect((await coordinator.availability(revision, day + 60_000)).allowed).toBe(false);
    expect((await coordinator.availability(revision, Date.UTC(2026, 8, 21))).allowed).toBe(true);
    database.close();
  });

  it("sums daily ledgers across a Monday-based weekly policy", async () => {
    const { coordinator, database } = harness();
    const weekly = { ...revision, policy: { limit: 100, period: "week" as const } };
    const monday = Date.UTC(2026, 8, 14, 12);
    const friday = Date.UTC(2026, 8, 18, 12);
    await coordinator.charge({ ...weekly, jobId: jobA, stepIndex: 0, tokens: 60, completedAt: monday }, monday);
    expect(await coordinator.charge({ ...weekly, jobId: jobB, stepIndex: 0, tokens: 40, completedAt: friday }, friday)).toMatchObject({
      allowed: false, used: 100, resetAt: Date.UTC(2026, 8, 21),
    });
    database.close();
  });

  it("wakes suspended jobs FIFO when a newer limit restores availability", async () => {
    const resumed: string[] = [];
    const { coordinator, database } = harness(async (jobId) => {
      resumed.push(jobId);
      return { state: "accepted" };
    });
    await coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: 100, completedAt: day }, day);
    await coordinator.suspend(jobB, day + 2);
    await coordinator.suspend(jobA, day + 1);
    expect(resumed).toEqual([]);
    const raised = { digest: "b".repeat(64), sortKey: "20260920T120001.000Z", policy: { limit: 1_000, period: "day" as const } };
    await coordinator.configure(raised, day + 3);
    expect(resumed).toEqual([jobA, jobB]);
    expect(database.prepare("SELECT COUNT(*) AS count FROM waiters").get()).toEqual({ count: 0 });
    database.close();
  });

  it("removing the policy preserves usage but unblocks waiters", async () => {
    const resumed: string[] = [];
    const { coordinator, database } = harness(async (jobId) => {
      resumed.push(jobId);
      return { state: "accepted" };
    });
    await coordinator.charge({ ...revision, jobId: jobA, stepIndex: 0, tokens: 100, completedAt: day }, day);
    await coordinator.suspend(jobB, day + 1);
    await coordinator.configure({ digest: "c".repeat(64), sortKey: "20260920T120002.000Z", policy: null }, day + 2);
    expect(resumed).toEqual([jobB]);
    expect(database.prepare("SELECT tokens FROM daily_usage").get()).toEqual({ tokens: 100 });
    database.close();
  });
});
