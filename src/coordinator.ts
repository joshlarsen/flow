import { DurableObject, tracing } from "cloudflare:workers";
import { appConfig } from "./generated-config.ts";
import type { CapacityResult, Env } from "./types.ts";
import {
  setFlowTraceAttributes,
  spanNames,
  traceAsync,
  type TraceSpan,
} from "./tracing.ts";

interface IdempotencyRow extends Record<string, SqlStorageValue> {
  job_id: string;
  request_hash: string;
}

interface CountRow extends Record<string, SqlStorageValue> {
  count: number;
}

interface MinimumExpiryRow extends Record<string, SqlStorageValue> {
  expires_at: number | null;
}

export class JobCoordinator extends DurableObject<Env> {
  /** Initializes the global capacity leases and idempotency-key indexes. */
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, expires_at INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS leases (
        job_id TEXT PRIMARY KEY,
        expires_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS idempotency_keys (
        key_hash TEXT PRIMARY KEY,
        prompt_hash TEXT NOT NULL,
        job_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idempotency_expiry ON idempotency_keys(expires_at);
    `);
  }

  /** Acquires capacity for a job or resolves an existing idempotent request. */
  async acquire(
    jobId: string,
    keyHash: string | null,
    requestHash: string,
    now: number,
    workflowTimeoutMs: number,
  ): Promise<CapacityResult> {
    return traceAsync(
      tracing,
      spanNames.capacityAcquire,
      {
        "agent_runner.job.id": jobId,
        "flow.trace": true,
        "flow.run.id": jobId,
        "agent_runner.idempotency.present": keyHash !== null,
      },
      (span) =>
        this.acquireCapacity(
          jobId,
          keyHash,
          requestHash,
          now,
          workflowTimeoutMs,
          span,
        ),
    );
  }

  /** Performs one atomic admission decision inside the coordinator object. */
  private async acquireCapacity(
    jobId: string,
    keyHash: string | null,
    requestHash: string,
    now: number,
    workflowTimeoutMs: number,
    span: TraceSpan,
  ): Promise<CapacityResult> {
    if (!Number.isSafeInteger(workflowTimeoutMs) || workflowTimeoutMs < 1)
      throw new Error("workflow timeout is invalid");
    this.cleanup(now);
    const existingLease =
      this.ctx.storage.sql
        .exec<CountRow>(
          "SELECT COUNT(*) AS count FROM leases WHERE job_id = ?",
          jobId,
        )
        .one()?.count ?? 0;
    if (existingLease > 0) {
      this.ctx.storage.sql.exec(
        "UPDATE leases SET expires_at = ? WHERE job_id = ?",
        now + workflowTimeoutMs + appConfig.shutdownGraceMs + 5 * 60_000,
        jobId,
      );
      await this.scheduleCleanup();
      span.setAttribute("agent_runner.capacity.result", "resumed");
      span.setAttribute("agent_runner.outcome", "success");
      return { kind: "acquired", jobId };
    }
    if (keyHash) {
      const existing = this.ctx.storage.sql
        .exec<IdempotencyRow>(
          "SELECT job_id, prompt_hash AS request_hash FROM idempotency_keys WHERE key_hash = ?",
          keyHash,
        )
        .toArray()[0];
      if (existing) {
        const requestMatches = existing.request_hash === requestHash;
        if (requestMatches && existing.job_id === jobId) {
          const retained =
            this.ctx.storage.sql
              .exec<CountRow>(
                "SELECT COUNT(*) AS count FROM leases WHERE job_id = ?",
                jobId,
              )
              .one()?.count ?? 0;
          if (retained > 0) {
            span.setAttribute("agent_runner.capacity.result", "resumed");
            span.setAttribute("agent_runner.outcome", "success");
            return { kind: "acquired", jobId };
          }
          const active =
            this.ctx.storage.sql
              .exec<CountRow>("SELECT COUNT(*) AS count FROM leases")
              .one()?.count ?? 0;
          if (active >= appConfig.maxInstances) {
            span.setAttribute("agent_runner.capacity.result", "full");
            span.setAttribute("agent_runner.outcome", "rejected");
            return { kind: "full" };
          }
          const leaseExpiresAt =
            now + workflowTimeoutMs + appConfig.shutdownGraceMs + 5 * 60_000;
          this.ctx.storage.sql.exec(
            "INSERT INTO leases(job_id, expires_at) VALUES (?, ?)",
            jobId,
            leaseExpiresAt,
          );
          await this.scheduleCleanup();
          span.setAttribute("agent_runner.capacity.result", "resumed");
          span.setAttribute("agent_runner.outcome", "success");
          return { kind: "acquired", jobId };
        }
        span.setAttribute("agent_runner.capacity.result", "existing");
        span.setAttribute("agent_runner.idempotency.matches", requestMatches);
        span.setAttribute("agent_runner.outcome", "success");
        return { kind: "existing", jobId: existing.job_id, requestMatches };
      }
    }

    const active =
      this.ctx.storage.sql
        .exec<CountRow>("SELECT COUNT(*) AS count FROM leases")
        .one()?.count ?? 0;
    if (active >= appConfig.maxInstances) {
      span.setAttribute("agent_runner.capacity.result", "full");
      span.setAttribute("agent_runner.outcome", "rejected");
      return { kind: "full" };
    }

    const leaseExpiresAt =
      now + workflowTimeoutMs + appConfig.shutdownGraceMs + 5 * 60_000;
    this.ctx.storage.sql.exec(
      "INSERT INTO leases(job_id, expires_at) VALUES (?, ?)",
      jobId,
      leaseExpiresAt,
    );
    if (keyHash) {
      this.ctx.storage.sql.exec(
        "INSERT INTO idempotency_keys(key_hash, prompt_hash, job_id, expires_at) VALUES (?, ?, ?, ?)",
        keyHash,
        requestHash,
        jobId,
        now + appConfig.retentionMs,
      );
    }
    await this.scheduleCleanup();
    span.setAttribute("agent_runner.capacity.result", "acquired");
    span.setAttribute("agent_runner.outcome", "success");
    return { kind: "acquired", jobId };
  }

  /** Releases a job's capacity lease and reschedules expiry cleanup. */
  async release(jobId: string): Promise<void> {
    return traceAsync(
      tracing,
      spanNames.capacityRelease,
      {
        "agent_runner.job.id": jobId,
      },
      async (span) => {
        setFlowTraceAttributes(span, jobId);
        this.ctx.storage.sql.exec("DELETE FROM leases WHERE job_id = ?", jobId);
        await this.scheduleCleanup();
        span.setAttribute("agent_runner.outcome", "success");
      },
    );
  }

  /** Rolls back a newly admitted job that was rejected by a later policy gate. */
  async rejectAdmission(jobId: string, keyHash: string | null): Promise<void> {
    this.ctx.storage.sql.exec("DELETE FROM leases WHERE job_id = ?", jobId);
    if (keyHash)
      this.ctx.storage.sql.exec(
        "DELETE FROM idempotency_keys WHERE key_hash = ? AND job_id = ?",
        keyHash,
        jobId,
      );
    await this.scheduleCleanup();
  }

  /** Removes expired leases and idempotency records when the alarm fires. */
  async alarm(): Promise<void> {
    return traceAsync(tracing, spanNames.capacityCleanup, {}, async (span) => {
      this.cleanup(Date.now());
      await this.scheduleCleanup();
      span.setAttribute("agent_runner.outcome", "success");
    });
  }

  async registerJob(
    jobId: string,
    createdAt: number,
    expiresAt: number,
  ): Promise<void> {
    this.ctx.storage.sql.exec(
      "INSERT INTO jobs(job_id,created_at,expires_at) VALUES (?,?,?) ON CONFLICT(job_id) DO UPDATE SET expires_at=MAX(expires_at,excluded.expires_at)",
      jobId,
      createdAt,
      expiresAt,
    );
    this.ctx.storage.sql.exec(
      "UPDATE idempotency_keys SET expires_at=MAX(expires_at,?) WHERE job_id=?",
      expiresAt,
      jobId,
    );
    await this.scheduleCleanup();
  }
  async listJobs(limit: number, cursor: string | null) {
    let position: { time: number; id: string } | null = null;
    if (cursor) {
      position = JSON.parse(atob(cursor));
      if (
        !position ||
        !Number.isSafeInteger(position.time) ||
        typeof position.id !== "string"
      )
        throw new Error("Invalid cursor");
    }
    const rows = position
      ? this.ctx.storage.sql
          .exec<{
            job_id: string;
            created_at: number;
          }>(
            "SELECT job_id,created_at FROM jobs WHERE expires_at>? AND (created_at<? OR (created_at=? AND job_id<?)) ORDER BY created_at DESC,job_id DESC LIMIT ?",
            Date.now(),
            position.time,
            position.time,
            position.id,
            limit + 1,
          )
          .toArray()
      : this.ctx.storage.sql
          .exec<{
            job_id: string;
            created_at: number;
          }>(
            "SELECT job_id,created_at FROM jobs WHERE expires_at>? ORDER BY created_at DESC,job_id DESC LIMIT ?",
            Date.now(),
            limit + 1,
          )
          .toArray();
    const items = rows.slice(0, limit),
      last = items.at(-1);
    return {
      items: items.map((row) => row.job_id),
      next_cursor:
        rows.length > limit && last
          ? btoa(JSON.stringify({ time: last.created_at, id: last.job_id }))
          : null,
    };
  }

  private cleanup(now: number): void {
    this.ctx.storage.sql.exec("DELETE FROM jobs WHERE expires_at <= ?", now);
    this.ctx.storage.sql.exec("DELETE FROM leases WHERE expires_at <= ?", now);
    this.ctx.storage.sql.exec(
      "DELETE FROM idempotency_keys WHERE expires_at <= ?",
      now,
    );
  }

  /** Schedules the next alarm for the earliest retained lease or key. */
  private async scheduleCleanup(): Promise<void> {
    const nextJob = this.ctx.storage.sql
      .exec<MinimumExpiryRow>("SELECT MIN(expires_at) AS expires_at FROM jobs")
      .one()?.expires_at;
    const nextLease = this.ctx.storage.sql
      .exec<MinimumExpiryRow>(
        "SELECT MIN(expires_at) AS expires_at FROM leases",
      )
      .one()?.expires_at;
    const nextKey = this.ctx.storage.sql
      .exec<MinimumExpiryRow>(
        "SELECT MIN(expires_at) AS expires_at FROM idempotency_keys",
      )
      .one()?.expires_at;
    const candidates = [nextJob, nextLease, nextKey].filter(
      (value): value is number => typeof value === "number",
    );
    if (candidates.length === 0) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.ctx.storage.setAlarm(
      Math.max(Date.now() + 1000, Math.min(...candidates)),
    );
  }
}
