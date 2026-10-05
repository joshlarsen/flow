import { DurableObject } from "cloudflare:workers";
import type { Env, WorkflowTokenBudget } from "./types.ts";
import { loadActiveWorkflowBundle } from "./workflow.ts";

export interface BudgetPolicyRevision {
  digest: string;
  sortKey: string;
  policy: WorkflowTokenBudget | null;
}

export interface BudgetAvailability {
  allowed: boolean;
  limit: number | null;
  used: number;
  resetAt: number | null;
  reason?: "exhausted" | "indeterminate" | "suspended_jobs";
}

export interface BudgetChargeInput extends BudgetPolicyRevision {
  jobId: string;
  stepIndex: number;
  tokens: number | null;
  completedAt: number;
}

interface PolicyRow extends Record<string, SqlStorageValue> {
  digest: string;
  sort_key: string;
  token_limit: number | null;
  period: string | null;
}

interface CountRow extends Record<string, SqlStorageValue> { count: number }
interface SumRow extends Record<string, SqlStorageValue> { total: number | null }
interface ChargeRow extends Record<string, SqlStorageValue> { tokens: number | null }
interface WaiterRow extends Record<string, SqlStorageValue> {
  job_id: string;
  suspended_at: number;
}

export interface BudgetResumeResult {
  state: "accepted" | "deferred" | "gone";
}

/** Returns the UTC calendar window containing the supplied timestamp. */
export function tokenBudgetWindow(now: number, period: WorkflowTokenBudget["period"]): { start: number; end: number } {
  const date = new Date(now);
  let start = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
  if (period === "week") {
    const weekday = new Date(start).getUTCDay();
    start -= ((weekday + 6) % 7) * 24 * 60 * 60_000;
  }
  return { start, end: start + (period === "day" ? 1 : 7) * 24 * 60 * 60_000 };
}

function validPolicyRevision(value: BudgetPolicyRevision): boolean {
  return /^[0-9a-f]{64}$/.test(value.digest) && /^\d{8}T\d{6}\.\d{3}Z$/.test(value.sortKey) &&
    (value.policy === null || Number.isSafeInteger(value.policy.limit) && value.policy.limit > 0 && (value.policy.period === "day" || value.policy.period === "week"));
}

/** Coordinates one workflow-name quota, its idempotent charges, and suspended-job wakeups. */
export class WorkflowBudgetCoordinator extends DurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS policy (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        digest TEXT NOT NULL,
        sort_key TEXT NOT NULL,
        token_limit INTEGER,
        period TEXT
      );
      CREATE TABLE IF NOT EXISTS daily_usage (
        day_start INTEGER PRIMARY KEY,
        tokens INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS charges (
        job_id TEXT NOT NULL,
        step_index INTEGER NOT NULL,
        day_start INTEGER NOT NULL,
        tokens INTEGER,
        completed_at INTEGER NOT NULL,
        PRIMARY KEY(job_id, step_index)
      );
      CREATE TABLE IF NOT EXISTS indeterminate_days (
        day_start INTEGER PRIMARY KEY
      );
      CREATE TABLE IF NOT EXISTS waiters (
        job_id TEXT PRIMARY KEY,
        suspended_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS budget_waiter_order ON waiters(suspended_at, job_id);
    `);
  }

  /** Registers a newer immutable bundle policy and wakes jobs when it restores availability. */
  async configure(revision: BudgetPolicyRevision, now = Date.now()): Promise<BudgetAvailability> {
    if (!validPolicyRevision(revision) || !Number.isSafeInteger(now) || now < 0) throw new Error("workflow budget policy is invalid");
    this.applyRevision(revision);
    const availability = this.currentAvailability(now, false);
    if (availability.allowed) await this.dispatchWaiters(now);
    await this.scheduleNext(now);
    return availability;
  }

  /** Checks quota admission without reserving tokens. Older suspended jobs retain priority. */
  async availability(revision: BudgetPolicyRevision, now = Date.now()): Promise<BudgetAvailability> {
    await this.configure(revision, now);
    return this.currentAvailability(now, true);
  }

  /** Applies one idempotent completed-step charge and returns the resulting period state. */
  async charge(input: BudgetChargeInput, now = Date.now()): Promise<BudgetAvailability> {
    if (!validPolicyRevision(input) || !Number.isSafeInteger(input.stepIndex) || input.stepIndex < 0 ||
      !Number.isSafeInteger(input.completedAt) || input.completedAt < 0 ||
      !Number.isSafeInteger(now) || now < 0 ||
      !(input.tokens === null || Number.isSafeInteger(input.tokens) && input.tokens >= 0) ||
      !/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(input.jobId)) throw new Error("workflow budget charge is invalid");
    this.applyRevision(input);
    const existing = this.ctx.storage.sql.exec<ChargeRow>(
      "SELECT tokens FROM charges WHERE job_id = ? AND step_index = ?",
      input.jobId, input.stepIndex,
    ).toArray()[0];
    if (existing && existing.tokens !== input.tokens) throw new Error("workflow budget charge conflicts with its recorded value");
    if (!existing) {
      const day = tokenBudgetWindow(now, "day").start;
      this.ctx.storage.sql.exec(
        "INSERT INTO charges(job_id, step_index, day_start, tokens, completed_at) VALUES (?, ?, ?, ?, ?)",
        input.jobId, input.stepIndex, day, input.tokens, now,
      );
      if (input.tokens === null) {
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO indeterminate_days(day_start) VALUES (?)", day);
      } else {
        const previous = this.ctx.storage.sql.exec<SumRow>("SELECT tokens AS total FROM daily_usage WHERE day_start = ?", day).toArray()[0]?.total ?? 0;
        if (!Number.isSafeInteger(previous + input.tokens)) throw new Error("workflow budget usage overflow");
        this.ctx.storage.sql.exec(
          `INSERT INTO daily_usage(day_start, tokens) VALUES (?, ?)
           ON CONFLICT(day_start) DO UPDATE SET tokens = excluded.tokens`,
          day, previous + input.tokens,
        );
      }
    }
    this.cleanup(now);
    await this.scheduleNext(now);
    return this.currentAvailability(now, false);
  }

  /** Adds a fully checkpointed job to the FIFO wake queue. */
  async suspend(jobId: string, suspendedAt = Date.now()): Promise<BudgetAvailability> {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(jobId) || !Number.isSafeInteger(suspendedAt) || suspendedAt < 0) throw new Error("budget waiter is invalid");
    this.ctx.storage.sql.exec("INSERT OR IGNORE INTO waiters(job_id, suspended_at) VALUES (?, ?)", jobId, suspendedAt);
    const availability = this.currentAvailability(suspendedAt, false);
    if (availability.allowed) await this.dispatchWaiters(suspendedAt);
    await this.scheduleNext(suspendedAt);
    return availability;
  }

  /** Removes a cancelled, expired, or successfully resumed job from the queue. */
  async remove(jobId: string): Promise<void> {
    this.ctx.storage.sql.exec("DELETE FROM waiters WHERE job_id = ?", jobId);
    await this.scheduleNext(Date.now());
  }

  async alarm(): Promise<void> {
    const now = Date.now();
    await this.reconcileActivePolicy();
    await this.dispatchWaiters(now);
    this.cleanup(now);
    await this.scheduleNext(now);
  }

  private policyRow(): PolicyRow | undefined {
    return this.ctx.storage.sql.exec<PolicyRow>("SELECT * FROM policy WHERE singleton = 1").toArray()[0];
  }

  private async reconcileActivePolicy(): Promise<void> {
    try {
      const bundle = await loadActiveWorkflowBundle(this.env);
      const current = this.policyRow();
      if (!current || bundle.manifest.workflow.name !== this.ctx.id.name) return;
      this.applyRevision({
        digest: bundle.manifest.digest,
        sortKey: bundle.manifest.sort_key,
        policy: bundle.manifest.workflow.token_budget ?? null,
      });
    } catch {
      /* Keep the last validated policy and retry on the next alarm. */
    }
  }

  private applyRevision(revision: BudgetPolicyRevision): void {
    const current = this.policyRow();
    const revisionKey = `${revision.sortKey}:${revision.digest}`;
    const currentKey = current ? `${current.sort_key}:${current.digest}` : null;
    if (currentKey && revisionKey < currentKey) return;
    this.ctx.storage.sql.exec(
      `INSERT INTO policy(singleton, digest, sort_key, token_limit, period) VALUES (1, ?, ?, ?, ?)
       ON CONFLICT(singleton) DO UPDATE SET digest = excluded.digest, sort_key = excluded.sort_key,
         token_limit = excluded.token_limit, period = excluded.period`,
      revision.digest, revision.sortKey, revision.policy?.limit ?? null, revision.policy?.period ?? null,
    );
  }

  private currentAvailability(now: number, respectWaiters: boolean): BudgetAvailability {
    const row = this.policyRow();
    if (!row || row.token_limit === null || row.period === null) return { allowed: true, limit: null, used: 0, resetAt: null };
    const policy = { limit: row.token_limit, period: row.period as WorkflowTokenBudget["period"] };
    const window = tokenBudgetWindow(now, policy.period);
    const indeterminate = (this.ctx.storage.sql.exec<CountRow>(
      "SELECT COUNT(*) AS count FROM indeterminate_days WHERE day_start >= ? AND day_start < ?",
      window.start, window.end,
    ).one()?.count ?? 0) > 0;
    const used = this.ctx.storage.sql.exec<SumRow>(
      "SELECT COALESCE(SUM(tokens), 0) AS total FROM daily_usage WHERE day_start >= ? AND day_start < ?",
      window.start, window.end,
    ).one()?.total ?? 0;
    const hasWaiters = respectWaiters && (this.ctx.storage.sql.exec<CountRow>("SELECT COUNT(*) AS count FROM waiters").one()?.count ?? 0) > 0;
    if (hasWaiters) return { allowed: false, limit: policy.limit, used, resetAt: Math.min(window.end, now + 30_000), reason: "suspended_jobs" };
    if (indeterminate) return { allowed: false, limit: policy.limit, used, resetAt: window.end, reason: "indeterminate" };
    return used < policy.limit
      ? { allowed: true, limit: policy.limit, used, resetAt: window.end }
      : { allowed: false, limit: policy.limit, used, resetAt: window.end, reason: "exhausted" };
  }

  private async dispatchWaiters(now: number): Promise<void> {
    while (this.currentAvailability(now, false).allowed) {
      const waiter = this.ctx.storage.sql.exec<WaiterRow>(
        "SELECT job_id, suspended_at FROM waiters ORDER BY suspended_at, job_id LIMIT 1",
      ).toArray()[0];
      if (!waiter) return;
      this.ctx.storage.sql.exec("DELETE FROM waiters WHERE job_id = ?", waiter.job_id);
      let result: BudgetResumeResult;
      try {
        const stub = this.env.AGENT_CONTAINER.get(this.env.AGENT_CONTAINER.idFromName(waiter.job_id));
        result = await stub.resumeBudgetJob();
      } catch {
        result = { state: "deferred" };
      }
      if (result.state === "deferred") {
        this.ctx.storage.sql.exec("INSERT OR IGNORE INTO waiters(job_id, suspended_at) VALUES (?, ?)", waiter.job_id, waiter.suspended_at);
        await this.ctx.storage.setAlarm(now + 30_000);
        return;
      }
    }
  }

  private cleanup(now: number): void {
    const cutoff = tokenBudgetWindow(now - 14 * 24 * 60 * 60_000, "day").start;
    this.ctx.storage.sql.exec("DELETE FROM daily_usage WHERE day_start < ?", cutoff);
    this.ctx.storage.sql.exec("DELETE FROM indeterminate_days WHERE day_start < ?", cutoff);
    this.ctx.storage.sql.exec("DELETE FROM charges WHERE completed_at < ?", cutoff);
  }

  private async scheduleNext(now: number): Promise<void> {
    const waiterCount = this.ctx.storage.sql.exec<CountRow>("SELECT COUNT(*) AS count FROM waiters").one()?.count ?? 0;
    if (waiterCount === 0) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    const availability = this.currentAvailability(now, false);
    const policyCheckAt = now + 30_000;
    await this.ctx.storage.setAlarm(Math.max(now + 1000, availability.allowed ? now + 1000 : Math.min(availability.resetAt ?? policyCheckAt, policyCheckAt)));
  }
}
