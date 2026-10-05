import { DurableObject, tracing } from "cloudflare:workers";
import { appConfig } from "./generated-config.ts";
import { sha256 } from "./http.ts";
import { submitWorkflowJob } from "./job-submission.ts";
import { spanNames, traceAsync, type TraceSpan } from "./tracing.ts";
import type { Env } from "./types.ts";
import { loadPinnedWorkflowBundle } from "./workflow.ts";

export interface ScheduledOccurrence {
  scheduleId: string;
  cron: string;
  scheduledAt: number;
  workflow: string;
  workflowDigest: string;
  manifestKey: string;
}

interface PendingScheduleRow extends Record<string, SqlStorageValue> {
  schedule_id: string;
  cron: string;
  scheduled_at: number;
  workflow: string;
  workflow_digest: string;
  manifest_key: string;
  job_id: string;
  expires_at: number;
  next_attempt_at: number;
  attempts: number;
  defer_reason: string | null;
}

interface MinimumTimeRow extends Record<string, SqlStorageValue> {
  value: number | null;
}

const capacityRetryMs = 30_000;
const deployedSchedules = appConfig.workflowSchedules as ReadonlyArray<{ readonly id: string; readonly cron: string }>;

function logSchedule(event: string, row: Pick<PendingScheduleRow, "schedule_id" | "cron" | "scheduled_at" | "workflow_digest">, detail: object = {}): void {
  console.log(JSON.stringify({
    level: event.endsWith("failed") ? "error" : event.endsWith("expired") ? "warn" : "info",
    source: "worker",
    event,
    schedule_id: row.schedule_id,
    cron: row.cron,
    scheduled_at: new Date(row.scheduled_at).toISOString(),
    workflow_version: row.workflow_digest,
    ...detail,
  }));
}

function validOccurrence(value: ScheduledOccurrence): boolean {
  const schedule = deployedSchedules.find((item) => item.id === value.scheduleId && item.cron === value.cron);
  return Boolean(schedule) && Number.isSafeInteger(value.scheduledAt) && value.scheduledAt > 0 &&
    /^[a-z][a-z0-9_-]{0,63}$/.test(value.workflow) && /^[0-9a-f]{64}$/.test(value.workflowDigest) &&
    value.manifestKey.length <= 1024;
}

export class ScheduleCoordinator extends DurableObject<Env> {
  /** Initializes the coalesced queue of capacity-blocked scheduled occurrences. */
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS pending_schedules (
        schedule_id TEXT PRIMARY KEY,
        cron TEXT NOT NULL,
        scheduled_at INTEGER NOT NULL,
        workflow TEXT NOT NULL,
        workflow_digest TEXT NOT NULL,
        manifest_key TEXT NOT NULL,
        job_id TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        next_attempt_at INTEGER NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0
        ,defer_reason TEXT
      );
      CREATE INDEX IF NOT EXISTS pending_schedule_attempt ON pending_schedules(next_attempt_at);
      CREATE INDEX IF NOT EXISTS pending_schedule_expiry ON pending_schedules(expires_at);
    `);

  }

  /** Durably records a cron occurrence, replacing only an older pending occurrence for the same schedule. */
  async enqueue(occurrence: ScheduledOccurrence): Promise<void> {
    if (!validOccurrence(occurrence)) throw new Error("Scheduled occurrence is invalid or is not deployed");
    const now = Date.now();
    const row = {
      schedule_id: occurrence.scheduleId,
      cron: occurrence.cron,
      scheduled_at: occurrence.scheduledAt,
      workflow_digest: occurrence.workflowDigest,
    };
    await traceAsync(tracing, spanNames.scheduleEnqueue, {
      "agent_runner.schedule.id": occurrence.scheduleId,
      "agent_runner.schedule.cron": occurrence.cron,
      "agent_runner.schedule.scheduled_at": occurrence.scheduledAt,
      "agent_runner.workflow.version": occurrence.workflowDigest,
    }, async (span) => {
      const previous = this.ctx.storage.sql
        .exec<PendingScheduleRow>("SELECT * FROM pending_schedules WHERE schedule_id = ?", occurrence.scheduleId)
        .toArray()[0];
      this.ctx.storage.sql.exec(
        `INSERT INTO pending_schedules(schedule_id, cron, scheduled_at, workflow, workflow_digest, manifest_key, job_id, expires_at, next_attempt_at, attempts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
         ON CONFLICT(schedule_id) DO UPDATE SET
           cron = excluded.cron, scheduled_at = excluded.scheduled_at, workflow = excluded.workflow,
           workflow_digest = excluded.workflow_digest, manifest_key = excluded.manifest_key,
           job_id = excluded.job_id, expires_at = excluded.expires_at,
           next_attempt_at = excluded.next_attempt_at, attempts = 0, defer_reason = NULL
         WHERE excluded.scheduled_at > pending_schedules.scheduled_at`,
        occurrence.scheduleId,
        occurrence.cron,
        occurrence.scheduledAt,
        occurrence.workflow,
        occurrence.workflowDigest,
        occurrence.manifestKey,
        crypto.randomUUID(),
        occurrence.scheduledAt + appConfig.retentionMs,
        now,
      );
      await this.scheduleNextAlarm();
      const event = !previous
        ? "scheduled_enqueued"
        : occurrence.scheduledAt > previous.scheduled_at
          ? "scheduled_coalesced"
          : "scheduled_duplicate";
      logSchedule(event, row, previous && event === "scheduled_coalesced"
        ? { replaced_scheduled_at: new Date(previous.scheduled_at).toISOString() }
        : {});
      span.setAttribute("agent_runner.outcome", "success");
    });
  }

  /** Dispatches one due occurrence and always retains an alarm for remaining work. */
  async alarm(): Promise<void> {
    return traceAsync(tracing, spanNames.scheduleDispatch, {}, async (span) => {
      const now = Date.now();
      this.expirePending(now);
      const row = this.ctx.storage.sql
        .exec<PendingScheduleRow>("SELECT * FROM pending_schedules WHERE next_attempt_at <= ? ORDER BY next_attempt_at, scheduled_at LIMIT 1", now)
        .toArray()[0];
      if (row) await this.dispatch(row, span, now);
      await this.scheduleNextAlarm();
      if (!row) span.setAttribute("agent_runner.outcome", "noop");
    });
  }

  /** Resolves a pinned workflow and attempts one globally capacity-limited admission. */
  private async dispatch(row: PendingScheduleRow, span: TraceSpan, now: number): Promise<void> {
    span.setAttribute("agent_runner.schedule.id", row.schedule_id);
    span.setAttribute("agent_runner.schedule.cron", row.cron);
    span.setAttribute("agent_runner.schedule.scheduled_at", row.scheduled_at);
    span.setAttribute("agent_runner.workflow.version", row.workflow_digest);
    if (!deployedSchedules.some((schedule) => schedule.id === row.schedule_id && schedule.cron === row.cron)) {
      this.deleteOccurrence(row);
      logSchedule("scheduled_removed", row);
      span.setAttribute("agent_runner.outcome", "rejected");
      return;
    }
    try {
      const bundle = await loadPinnedWorkflowBundle(this.env, row.manifest_key, row.workflow_digest);
      const occurrenceKey = `cron:v1:${appConfig.deploymentName}:${row.workflow}:${row.schedule_id}:${row.scheduled_at}`;
      const submission = await submitWorkflowJob(this.env, {
        jobId: row.job_id,
        resumeExisting: true,
        bundle,
        keyHash: await sha256(occurrenceKey),
        requestHash: await sha256(bundle.manifest.digest),
        callbackBaseURL: appConfig.runnerUrl ?? "",
        trigger: {
          type: "cron",
          schedule_id: row.schedule_id,
          cron: row.cron,
          scheduled_at: new Date(row.scheduled_at).toISOString(),
        },
      });
      if (submission.kind === "full") {
        this.defer(row, now + capacityRetryMs, false, "capacity");
        logSchedule("scheduled_capacity_deferred", row);
        span.setAttribute("agent_runner.outcome", "deferred");
        return;
      }
      if (submission.kind === "budget_exhausted") {
        this.defer(row, submission.budget.resetAt ?? now + capacityRetryMs, false, "budget");
        logSchedule("scheduled_budget_deferred", row, { reset_at: submission.budget.resetAt ? new Date(submission.budget.resetAt).toISOString() : null });
        span.setAttribute("agent_runner.outcome", "deferred");
        return;
      }
      if (submission.kind === "start_failed" && !submission.authoritative) {
        this.defer(row, now + this.retryDelay(row.attempts), true, "startup");
        logSchedule("scheduled_dispatch_failed", row, { retryable: true });
        span.setAttribute("agent_runner.outcome", "retry");
        return;
      }
      this.deleteOccurrence(row);
      if (submission.kind === "start_failed") {
        logSchedule("scheduled_dispatch_failed", row, { job_id: submission.jobId, retryable: false });
        span.setAttribute("agent_runner.outcome", "failed");
        return;
      }
      logSchedule("scheduled_dispatched", row, { job_id: submission.jobId, result: submission.kind });
      span.setAttribute("agent_runner.job.id", submission.jobId);
      span.setAttribute("agent_runner.outcome", submission.kind === "conflict" || submission.kind === "missing" ? "rejected" : "success");
    } catch (error) {
      this.defer(row, now + this.retryDelay(row.attempts), true, "error");
      logSchedule("scheduled_dispatch_failed", row, { retryable: true, error_type: error instanceof Error ? error.name : typeof error });
      span.setAttribute("agent_runner.outcome", "retry");
    }
  }

  private retryDelay(attempts: number): number {
    return Math.min(300_000, 2_000 * 2 ** Math.min(attempts, 7));
  }

  private defer(row: PendingScheduleRow, nextAttemptAt: number, incrementAttempts: boolean, reason: string): void {
    this.ctx.storage.sql.exec(
      `UPDATE pending_schedules SET next_attempt_at = ?, attempts = attempts + ?, defer_reason = ?
       WHERE schedule_id = ? AND scheduled_at = ?`,
      nextAttemptAt,
      Number(incrementAttempts),
      reason,
      row.schedule_id,
      row.scheduled_at,
    );
  }

  /** Makes budget-deferred occurrences immediately eligible after a policy update. */
  async wakeBudgetDeferred(workflow: string): Promise<number> {
    if (!/^[a-z][a-z0-9_-]{0,63}$/.test(workflow)) throw new Error("workflow name is invalid");
    const count = this.ctx.storage.sql.exec<{ count: number }>(
      "SELECT COUNT(*) AS count FROM pending_schedules WHERE workflow = ? AND defer_reason = 'budget'",
      workflow,
    ).one()?.count ?? 0;
    if (count > 0) {
      this.ctx.storage.sql.exec(
        "UPDATE pending_schedules SET next_attempt_at = ?, defer_reason = NULL WHERE workflow = ? AND defer_reason = 'budget'",
        Date.now(), workflow,
      );
      await this.scheduleNextAlarm();
    }
    return count;
  }

  private deleteOccurrence(row: PendingScheduleRow): void {
    this.ctx.storage.sql.exec("DELETE FROM pending_schedules WHERE schedule_id = ? AND scheduled_at = ?", row.schedule_id, row.scheduled_at);
  }

  /** Removes expired work while leaving a structured operational record. */
  private expirePending(now: number): void {
    const expired = this.ctx.storage.sql.exec<PendingScheduleRow>("SELECT * FROM pending_schedules WHERE expires_at <= ?", now).toArray();
    for (const row of expired) logSchedule("scheduled_expired", row);
    this.ctx.storage.sql.exec("DELETE FROM pending_schedules WHERE expires_at <= ?", now);
  }

  private async scheduleNextAlarm(): Promise<void> {
    const nextAttempt = this.ctx.storage.sql.exec<MinimumTimeRow>("SELECT MIN(next_attempt_at) AS value FROM pending_schedules").one()?.value;
    const nextExpiry = this.ctx.storage.sql.exec<MinimumTimeRow>("SELECT MIN(expires_at) AS value FROM pending_schedules").one()?.value;
    const candidates = [nextAttempt, nextExpiry].filter((value): value is number => typeof value === "number");
    if (candidates.length === 0) {
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.ctx.storage.setAlarm(Math.max(Date.now() + 1000, Math.min(...candidates)));
  }
}
