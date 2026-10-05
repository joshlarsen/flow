export const ACTIVE_JOB_STATUSES = [
  "starting",
  "running",
  "waiting_for_input",
  "sleeping",
  "budget_suspended",
  "resuming",
  "cancelling",
  "finalizing",
] as const;
export const TERMINAL_JOB_STATUSES = [
  "succeeded",
  "partial",
  "failed",
  "timed_out",
  "cancelled",
  "interrupted",
] as const;
export const JOB_STATUSES = [
  ...ACTIVE_JOB_STATUSES,
  ...TERMINAL_JOB_STATUSES,
] as const;

export type JobStatus = (typeof JOB_STATUSES)[number];

export interface SessionUsage {
  total_tokens?: number;
  total_input_tokens?: number;
  input_tokens?: number;
  output_tokens?: number;
  thought_tokens?: number;
  cached_read_tokens?: number;
  cached_write_tokens?: number;
  used?: number;
  size?: number;
}

export interface WorkflowTokenBudget {
  limit: number;
  period: "day" | "week";
}

export interface JobError {
  code: string;
  message: string;
  retryable: boolean;
}

export type ReasoningEffort =
  | "none"
  | "minimal"
  | "low"
  | "medium"
  | "high"
  | "xhigh"
  | "max";

interface BaseStepResult {
  id: string;
  status:
    | "pending"
    | "running"
    | "succeeded"
    | "partial"
    | "failed"
    | "timed_out"
    | "cancelled";
  started_at: string;
  finished_at: string;
  exit_code: number;
  error: JobError | null;
  emitted_metrics?: string[];
}

export interface AgentStepResult extends BaseStepResult {
  prompt: string;
  harness: string;
  provider: string;
  model: string;
  reasoning_effort: ReasoningEffort | null;
  session_id: string | null;
  stop_reason:
    | "end_turn"
    | "max_tokens"
    | "max_turn_requests"
    | "refusal"
    | "cancelled"
    | null;
  message: string;
  usage: SessionUsage | null;
}

export interface CommandStepResult extends BaseStepResult {
  command: string[];
  stdout: string;
  stderr: string;
}

export type StepResult = AgentStepResult | CommandStepResult;

export interface ArtifactMetadata {
  path: string;
  key: string;
  size: number;
  sha256: string;
  etag: string;
  content_type: string;
}

export interface MemoryMetadata {
  key: string;
  size: number;
  sha256: string;
  etag: string;
  persisted_at: string;
}

export interface WorkflowResult {
  steps: StepResult[];
  usage: SessionUsage | null;
  artifacts: ArtifactMetadata[];
  artifact_error?: JobError;
  memory?: MemoryMetadata;
  memory_error?: JobError;
}

export type HistoryState = ReturnType<
  typeof import("./history.ts").historyView
>;

export type JobTrigger =
  | { type: "api" }
  | { type: "cron"; schedule_id: string; cron: string; scheduled_at: string };

export interface JobView {
  job_id: string;
  workflow: string;
  workflow_version: string;
  trigger: JobTrigger;
  status: JobStatus;
  current_step: string | null;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  expires_at: string;
  resume_at?: string;
  history: HistoryState;
  pending_interaction?: import("./interactions.ts").PendingInteraction;
  result?: WorkflowResult;
  error?: JobError;
}

export interface RunnerCompletion {
  status:
    | "running"
    | "waiting_for_input"
    | "budget_suspended"
    | "succeeded"
    | "partial"
    | "failed"
    | "timed_out"
    | "cancelled";
  workflow: string;
  workflow_digest: string;
  steps: StepResult[];
  usage: SessionUsage | null;
  error: JobError | null;
  artifact_error?: JobError;
  memory_error?: JobError;
  event_sequence: number;
  budget_reset_at?: string;
}

export interface AgentWorkflowStep {
  id: string;
  prompt: string;
  allow_user_input?: boolean;
  harness: string;
  provider: string;
  model: string;
  reasoning_effort: ReasoningEffort | null;
  required_metrics: RequiredMetric[];
  timeout_ms: number;
}

export interface RequiredMetric {
  namespace: string;
  key: string;
  description: string;
}

export interface CommandWorkflowStep {
  id: string;
  command: string[];
  timeout_ms: number;
}

export type WorkflowStep = AgentWorkflowStep | CommandWorkflowStep;

export type WorkflowBundleFileKind = "prompt" | "script" | "skill";

export interface WorkflowBundleManifest {
  version: 1;
  digest: string;
  sort_key: string;
  workflow: {
    version: 1;
    name: string;
    memory_enabled?: boolean;
    workflow_timeout_ms: number;
    token_budget?: WorkflowTokenBudget;
    default_step_timeout_ms: number;
    default_harness?: string | null;
    default_model?: string | null;
    default_reasoning_effort?: ReasoningEffort | null;
    steps: WorkflowStep[];
  };
  archive: { key: string; size: number; sha256: string };
  files: Array<{
    kind: WorkflowBundleFileKind;
    path: string;
    size: number;
    sha256: string;
    executable: boolean;
  }>;
  total_bytes: number;
}

export interface WorkflowBundle {
  manifest: WorkflowBundleManifest;
}

export interface StartJobInput {
  jobId: string;
  traceId: string;
  rootSpanId: string;
  bundle: WorkflowBundle;
  requestHash: string;
  createdAt: number;
  callbackBaseURL: string;
  trigger: JobTrigger;
}

export interface CapacityResult {
  kind: "acquired" | "existing" | "full";
  jobId?: string;
  requestMatches?: boolean;
}

export interface Env {
  AGENT_CONTAINER: DurableObjectNamespace<
    import("./container.ts").AgentContainer
  >;
  WORKFLOW_BUDGET: DurableObjectNamespace<
    import("./workflow-budget.ts").WorkflowBudgetCoordinator
  >;
  JOB_COORDINATOR: DurableObjectNamespace<
    import("./coordinator.ts").JobCoordinator
  >;
  SCHEDULE_COORDINATOR: DurableObjectNamespace<
    import("./scheduler.ts").ScheduleCoordinator
  >;
  OAUTH_CREDENTIAL_BROKER: DurableObjectNamespace<
    import("./oauth-broker.ts").OAuthCredentialBroker
  >;
  RUNNER_STORAGE: R2Bucket;
  RUNNER_API_TOKEN: string;
}
