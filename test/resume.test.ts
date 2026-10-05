import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import {
  acceptResumeDispatchSQL,
  buildResumeRunRequest,
  MAX_RESUME_ATTEMPTS,
  prepareResumeDispatchSQL,
  recordResumeAttemptSQL,
  resumeStateProblem,
  shouldRetryResume,
} from "../src/resume.ts";

describe("cold resume state", () => {
  it("reports the exact missing persisted field", () => {
    const complete = {
      checkpointKey: "jobs/job-1/checkpoint/session.tgz",
      sessionId: "session-1",
      bundleManifestKey: "bundles/example/manifest.json",
      workflowVersion: "a".repeat(64),
      callbackBaseUrl: "https://runner.example.com",
      startedAt: Date.parse("2026-09-15T14:30:51.882Z"),
      remainingWorkflowMs: 50_000,
      remainingStepMs: 40_000,
    };
    expect(resumeStateProblem(complete)).toBeNull();
    for (const [field, value, expected] of [
      ["checkpointKey", null, /checkpoint/],
      ["sessionId", null, /ACP session ID/],
      ["bundleManifestKey", null, /workflow bundle/],
      ["workflowVersion", null, /workflow version/],
      ["callbackBaseUrl", null, /callback URL/],
      ["startedAt", null, /original run start time/],
      ["remainingWorkflowMs", null, /workflow budget/],
      ["remainingStepMs", 0, /step budget/],
    ] as const) {
      expect(resumeStateProblem({ ...complete, [field]: value })).toMatch(expected);
    }
  });

  it("allows exactly one retry after the first acquired attempt", () => {
    expect(MAX_RESUME_ATTEMPTS).toBe(2);
    expect(shouldRetryResume(0, true)).toBe(true);
    expect(shouldRetryResume(1, true)).toBe(true);
    expect(shouldRetryResume(2, true)).toBe(false);
    expect(shouldRetryResume(1, false)).toBe(false);
  });

  it("preserves the saved workflow budget until dispatch is accepted", () => {
    const database = new DatabaseSync(":memory:");
    database.exec(`
      CREATE TABLE job (
        singleton INTEGER PRIMARY KEY,
        status TEXT NOT NULL,
        callback_hash TEXT NOT NULL,
        workflow_deadline_at INTEGER,
        remaining_workflow_ms INTEGER,
        resume_attempts INTEGER NOT NULL
      );
      INSERT INTO job VALUES (1, 'resuming', 'old-hash', NULL, 50000, 0);
    `);

    database.prepare(recordResumeAttemptSQL).run(1);
    database.prepare(prepareResumeDispatchSQL).run("new-hash");
    expect(database.prepare("SELECT * FROM job").get()).toMatchObject({
      status: "resuming",
      callback_hash: "new-hash",
      workflow_deadline_at: null,
      remaining_workflow_ms: 50000,
      resume_attempts: 1,
    });

    database.prepare(acceptResumeDispatchSQL).run(100000);
    expect(database.prepare("SELECT status, workflow_deadline_at, remaining_workflow_ms FROM job").get()).toEqual({
      status: "running",
      workflow_deadline_at: 100000,
      remaining_workflow_ms: null,
    });
    database.close();
  });

  it("preserves the logical run start time in resumed requests", () => {
    const runStartedAt = Date.parse("2026-09-15T14:30:51.882Z");
    const deadlineAt = Date.parse("2026-09-15T14:45:00.000Z");
    const request = buildResumeRunRequest({
      jobId: "job-1",
      callbackToken: "callback-token",
      callbackBaseUrl: "https://runner.example.com",
      historyEnabled: true,
      budgetEnabled: false,
      runCreatedAt: Date.parse("2026-09-15T14:30:50.730Z"),
      runStartedAt,
      traceId: "1".repeat(32),
      runSpanId: "2".repeat(16),
      deadlineAt,
      stepIndex: 1,
      sessionId: "session-1",
      response: { action: "accept", content: { answer: "yes" } },
      steps: [],
      eventSequence: 72,
      remainingStepMs: 40_000,
    }) as Record<string, any>;

    expect(request.run_started_at).toBe("2026-09-15T14:30:51.882Z");
    expect(request.deadline_at).toBe("2026-09-15T14:45:00.000Z");
    expect(request.resume).toMatchObject({ step_index: 1, event_sequence: 72, remaining_step_ms: 40_000 });
  });
});
