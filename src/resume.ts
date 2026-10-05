import type { InteractionResponse } from "./interactions.ts";
import type { StepResult } from "./types.ts";

export const MAX_RESUME_ATTEMPTS = 2;

export const recordResumeAttemptSQL =
  "UPDATE job SET resume_attempts = ? WHERE singleton = 1";
export const prepareResumeDispatchSQL =
  "UPDATE job SET callback_hash = ? WHERE singleton = 1";
export const acceptResumeDispatchSQL = `UPDATE job
  SET status = 'running', workflow_deadline_at = ?, remaining_workflow_ms = NULL
  WHERE singleton = 1 AND status = 'resuming'`;

export interface SavedResumeState {
  checkpointKey: string | null;
  sessionId: string | null;
  bundleManifestKey: string | null;
  workflowVersion: string | null;
  callbackBaseUrl: string | null;
  startedAt: number | null;
  remainingWorkflowMs: number | null;
  remainingStepMs: number | null;
}

export interface ResumeRunRequestInput {
  jobId: string;
  callbackToken: string;
  callbackBaseUrl: string;
  historyEnabled: boolean;
  budgetEnabled: boolean;
  runCreatedAt: number;
  runStartedAt: number;
  traceId: string;
  runSpanId: string;
  deadlineAt: number;
  stepIndex: number;
  sessionId: string;
  response: InteractionResponse;
  steps: StepResult[];
  eventSequence: number;
  remainingStepMs: number;
}

/** Identifies the first permanent defect in a persisted cold-resume state. */
export function resumeStateProblem(state: SavedResumeState): string | null {
  if (!state.checkpointKey)
    return "The saved agent session is missing its checkpoint";
  if (!state.sessionId)
    return "The saved agent session is missing its ACP session ID";
  if (!state.bundleManifestKey)
    return "The saved agent session is missing its pinned workflow bundle";
  if (!state.workflowVersion)
    return "The saved agent session is missing its workflow version";
  if (!state.callbackBaseUrl)
    return "The saved agent session is missing its callback URL";
  if (state.startedAt === null)
    return "The saved agent session is missing the original run start time";
  if (state.remainingWorkflowMs === null || state.remainingWorkflowMs < 1)
    return "The saved agent session is missing its remaining workflow budget";
  if (state.remainingStepMs === null || state.remainingStepMs < 1)
    return "The saved agent session is missing its remaining step budget";
  return null;
}

export function shouldRetryResume(
  attempt: number,
  retryable: boolean,
): boolean {
  return retryable && attempt < MAX_RESUME_ATTEMPTS;
}

/** Builds a resumed runner request while preserving the logical run timestamps. */
export function buildResumeRunRequest(input: ResumeRunRequestInput): object {
  return {
    job_id: input.jobId,
    callback_token: input.callbackToken,
    callback_url: input.historyEnabled
      ? `${input.callbackBaseUrl}/internal/v1/jobs/${input.jobId}/events`
      : "",
    interaction_url: `${input.callbackBaseUrl}/internal/v1/jobs/${input.jobId}/interactions`,
    budget_url: input.budgetEnabled
      ? `${input.callbackBaseUrl}/internal/v1/jobs/${input.jobId}/token-usage`
      : "",
    run_harness: "workflow",
    run_created_at: new Date(input.runCreatedAt).toISOString(),
    run_started_at: new Date(input.runStartedAt).toISOString(),
    trace_id: input.traceId,
    run_span_id: input.runSpanId,
    deadline_at: new Date(input.deadlineAt).toISOString(),
    resume: {
      kind: "interaction",
      step_index: input.stepIndex,
      session_id: input.sessionId,
      response: input.response,
      steps: input.steps,
      event_sequence: input.eventSequence,
      remaining_step_ms: input.remainingStepMs,
    },
  };
}
