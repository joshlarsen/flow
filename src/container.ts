import {
  initializeHistory,
  appendHistory,
  archiveHistory,
  historyView,
  historyPage,
  HistoryError,
  type HistoryKind,
} from "./history.ts";
import { Container, type OutboundHandlerContext } from "@cloudflare/containers";
import { tracing } from "cloudflare:workers";
import { appConfig } from "./generated-config.ts";
import {
  containerProxyToken,
  credentialTarget,
  injectCredential as injectCredentialRequest,
} from "./credential-proxy.ts";
import { sha256 } from "./http.ts";
import { writeFixedLengthBody } from "./fixed-length-body.ts";
import { runtimeEnvironment } from "./runtime-environment.ts";
import {
  acceptResumeDispatchSQL,
  buildResumeRunRequest,
  prepareResumeDispatchSQL,
  recordResumeAttemptSQL,
  resumeStateProblem,
  shouldRetryResume,
} from "./resume.ts";
import {
  jobCheckpointKey,
  jobOutputKey,
  workflowMemoryKey,
} from "./storage-keys.ts";
import {
  loadPinnedWorkflowBundle,
  loadWorkflowBundleArchive,
  workflowBundleTelemetry,
  workflowManifestKey,
} from "./workflow.ts";
import type { BudgetResumeResult } from "./workflow-budget.ts";
import {
  completeInteraction,
  deliverInteraction,
  interactionActorAllowed,
  interactionRetryAfter,
  parseInteractionDelivery,
  type InteractionDelivery,
} from "./interaction-provider.ts";
import {
  interactionContinuesCurrentPrompt,
  parseElicitationRequest,
  validateElicitationContent,
  type ElicitationRequest,
  type InteractionOutcome,
  type InteractionProviderLookup,
  type InteractionProviderResolution,
  type InteractionResponse,
} from "./interactions.ts";
import {
  containerSleepAfter,
  workflowDeadlineAt,
  workflowDeadlineElapsed,
} from "./timeouts.ts";
import type {
  ArtifactMetadata,
  Env,
  JobError,
  JobStatus,
  JobView,
  MemoryMetadata,
  RunnerCompletion,
  SessionUsage,
  StepResult,
  StartJobInput,
} from "./types.ts";
import {
  recordDeliveredEventBatchSQL,
  recordReceivedEventSequenceSQL,
} from "./history-accounting.ts";
import {
  errorType,
  recordHttpResult,
  setFlowTraceAttributes,
  setSpanAttributes,
  spanNames,
  traceAsync,
  type TraceSpan,
} from "./tracing.ts";

interface JobRow extends Record<string, SqlStorageValue> {
  job_id: string;
  harness: string | null;
  workflow: string | null;
  workflow_version: string | null;
  workflow_default_harness: string | null;
  workflow_default_model: string | null;
  workflow_default_reasoning_effort: string | null;
  trigger_type: "api" | "cron";
  trigger_schedule_id: string | null;
  trigger_cron: string | null;
  trigger_scheduled_at: number | null;
  current_step: string | null;
  status: JobStatus;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  expires_at: number;
  callback_hash: string;
  session_id: string | null;
  stop_reason: string | null;
  result_message: string | null;
  usage_json: string | null;
  exit_code: number | null;
  error_code: string | null;
  error_message: string | null;
  error_retryable: number | null;
  steps_json: string | null;
  artifacts_json: string | null;
  artifact_error_json: string | null;
  memory_enabled: number;
  memory_key: string | null;
  memory_source_etag: string | null;
  memory_json: string | null;
  memory_error_json: string | null;
  finalization_deadline_at: number | null;
  event_received_batches: number;
  event_delivered_batches: number;
  event_last_error: string | null;
  event_updated_at: number | null;
  workflow_deadline_at: number | null;
  remaining_workflow_ms: number | null;
  remaining_step_ms: number | null;
  budget_reset_at: number | null;
  budget_next_step: number | null;
  bundle_manifest_key: string | null;
  callback_base_url: string | null;
  checkpoint_key: string | null;
  event_sequence: number;
  resume_attempts: number;
  trace_id: string;
  run_span_id: string;
}

interface OutboxRow extends Record<string, SqlStorageValue> {
  id: string;
  payload: string;
  attempts: number;
}

interface InteractionRow extends Record<string, SqlStorageValue> {
  id: string;
  status: "pending" | "resolved" | "expired";
  request_json: string;
  response_json: string | null;
  created_at: number;
  expires_at: number;
  resolved_at: number | null;
  provider_actor: string | null;
  provider_delivery_json: string | null;
  provider_message_updated_at: number | null;
  provider_message_attempts: number;
  provider_message_last_error: string | null;
}

export interface InteractionCallbackResult {
  accepted: boolean;
  state?: "resolved" | "pending";
  interaction_id?: string;
  response?: InteractionResponse;
  status?: number;
  code?: string;
  message?: string;
}

export interface TokenBudgetCallbackResult {
  accepted: boolean;
  action?: "continue" | "suspend";
  reset_at?: string;
  status?: number;
  code?: string;
  message?: string;
}

interface SupervisorArtifact {
  index: number;
  path: string;
  size: number;
  sha256: string;
  content_type: string;
}

interface EgressTraceContext {
  readonly jobId?: string;
  readonly workflow?: string;
  readonly traceId?: string;
}

function setJobAttributes(span: TraceSpan, row: JobRow | null): void {
  setFlowTraceAttributes(span, row?.job_id, row?.trace_id);
  setSpanAttributes(span, {
    "agent_runner.job.id": row?.job_id,
    "agent_runner.workflow.name": row?.workflow ?? undefined,
    "agent_runner.workflow.version": row?.workflow_version ?? undefined,
    "agent_runner.job.status": row?.status,
    "agent_runner.trigger.type": row?.trigger_type,
    "agent_runner.schedule.id": row?.trigger_schedule_id ?? undefined,
    "agent_runner.schedule.cron": row?.trigger_cron ?? undefined,
    "agent_runner.schedule.scheduled_at":
      row?.trigger_scheduled_at ?? undefined,
  });
}

function jobTrigger(row: JobRow): JobView["trigger"] {
  return row.trigger_type === "cron" &&
    row.trigger_schedule_id &&
    row.trigger_cron &&
    row.trigger_scheduled_at !== null
    ? {
        type: "cron",
        schedule_id: row.trigger_schedule_id,
        cron: row.trigger_cron,
        scheduled_at: new Date(row.trigger_scheduled_at).toISOString(),
      }
    : { type: "api" };
}

function unpersistedMemoryError(
  row: JobRow,
  message: string,
): JobError | undefined {
  return row.memory_enabled && !row.memory_json
    ? { code: "memory_persist_failed", message, retryable: true }
    : undefined;
}

class MemoryOperationError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = "MemoryOperationError";
  }
}

class ResumeOperationError extends Error {
  constructor(
    message: string,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = "ResumeOperationError";
  }
}

const terminalStatuses = new Set<JobStatus>([
  "succeeded",
  "partial",
  "failed",
  "timed_out",
  "cancelled",
  "interrupted",
]);
const encoder = new TextEncoder();
const flowRunHarness = "workflow";
const historyEnabled = true;
type FlowEventLevel = "info" | "warn" | "error";
type FlowSpanStatus = "UNSET" | "OK" | "ERROR";

interface FlowEventInput {
  type: string;
  sequence: number;
  level: FlowEventLevel;
  data: object;
  occurredAt?: number;
}

interface MemoryMaterializationMetadata {
  key: string;
  source: "r2" | "initialized";
  object: {
    size: number;
    etag: string;
    sha256: string | null;
    persisted_at: string;
  } | null;
}

const bundleMaterializedSequence = 1;
const memoryMaterializedSequence = 2;
const runStartedSequence = 3;

interface FlowSpanRecord {
  trace_id: string;
  span_id: string;
  parent_span_id: string | null;
  name: string;
  kind: "INTERNAL" | "CLIENT" | "SERVER";
  started_at: string;
  finished_at: string;
  status: { code: FlowSpanStatus };
  attributes: Record<string, string | number | boolean>;
}

interface FlowBatchEnvelope {
  schema_version?: unknown;
  run?: { source_run_id?: unknown; trace_id?: unknown; root_span_id?: unknown };
  events?: unknown;
  spans?: unknown;
  metrics?: unknown;
}

interface FlowBatchMetadata {
  maxSequence: number | null;
}

function lifecycleLevel(status: JobStatus): FlowEventLevel {
  if (
    status === "succeeded" ||
    status === "running" ||
    status === "waiting_for_input" ||
    status === "sleeping" ||
    status === "budget_suspended" ||
    status === "resuming" ||
    status === "starting"
  )
    return "info";
  if (status === "partial") return "warn";
  return "error";
}

function lifecycleSpanStatus(status: JobStatus): FlowSpanStatus {
  if (status === "succeeded") return "OK";
  if (status === "failed" || status === "timed_out") return "ERROR";
  return "UNSET";
}

function validHexId(value: unknown, length: number): value is string {
  return (
    typeof value === "string" &&
    value.length === length &&
    /^[0-9a-f]+$/.test(value) &&
    !/^0+$/.test(value)
  );
}

/** Accepts only protocol-v4 batches bound to this job's persisted trace context. */
function flowBatchMetadata(
  payload: string,
  row: JobRow,
): FlowBatchMetadata | null {
  try {
    const envelope = JSON.parse(payload) as FlowBatchEnvelope;
    if (
      !(
        envelope.schema_version === 4 &&
        envelope.run?.source_run_id === row.job_id &&
        validHexId(envelope.run.trace_id, 32) &&
        validHexId(envelope.run.root_span_id, 16) &&
        envelope.run.trace_id === row.trace_id &&
        envelope.run.root_span_id === row.run_span_id &&
        Array.isArray(envelope.events) &&
        Array.isArray(envelope.spans) &&
        Array.isArray(envelope.metrics)
      )
    )
      return null;
    const valid =
      envelope.events.every((value) => {
        if (!value || typeof value !== "object") return false;
        const event = value as Record<string, unknown>;
        return (
          event.protocol_version === 4 &&
          event.trace_id === row.trace_id &&
          validHexId(event.span_id, 16) &&
          typeof event.sequence === "number" &&
          Number.isSafeInteger(event.sequence) &&
          event.sequence >= 0
        );
      }) &&
      envelope.spans.every((value) => {
        if (!value || typeof value !== "object") return false;
        const item = value as Record<string, unknown>;
        return (
          item.trace_id === row.trace_id &&
          validHexId(item.span_id, 16) &&
          (item.parent_span_id === null || validHexId(item.parent_span_id, 16))
        );
      }) &&
      envelope.metrics.every((value) => {
        if (!value || typeof value !== "object") return false;
        const metric = value as Record<string, unknown>;
        return (
          metric.protocol_version === 4 &&
          metric.trace_id === row.trace_id &&
          validHexId(metric.span_id, 16) &&
          typeof metric.metric_id === "string" &&
          metric.metric_id.length > 0 &&
          typeof metric.sequence === "number" &&
          Number.isSafeInteger(metric.sequence) &&
          metric.sequence >= 0 &&
          typeof metric.namespace === "string" &&
          /^[a-z][a-z0-9_]{0,62}$/.test(metric.namespace) &&
          typeof metric.key === "string" &&
          /^[a-z][a-z0-9_]{0,62}$/.test(metric.key) &&
          typeof metric.value === "number" &&
          Number.isFinite(metric.value) &&
          typeof metric.occurred_at === "string" &&
          Number.isFinite(Date.parse(metric.occurred_at))
        );
      });
    if (!valid) return null;
    const sequences = envelope.events.map(
      (value) => (value as { sequence: number }).sequence,
    );
    return {
      maxSequence: sequences.length > 0 ? Math.max(...sequences) : null,
    };
  } catch {
    return null;
  }
}

/** Generates a high-entropy URL-safe token for internal result callbacks. */
function randomToken(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(32));
  return btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");
}

function toIso(value: number | null): string | null {
  return value === null ? null : new Date(value).toISOString();
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function safeArtifactPath(value: string): boolean {
  return (
    value.length > 0 &&
    !value.startsWith("/") &&
    !value.includes("\\") &&
    value
      .split("/")
      .every((part) => part !== "" && part !== "." && part !== "..")
  );
}

async function sha256Bytes(value: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new Uint8Array(value).buffer,
  );
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function hexadecimalBytes(value: string): Uint8Array {
  if (!/^[0-9a-f]{64}$/.test(value)) throw new Error("invalid SHA-256 digest");
  const bytes = new Uint8Array(32);
  for (let index = 0; index < bytes.length; index++)
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  return bytes;
}

function isUsage(value: unknown): value is SessionUsage | null {
  if (value === null) return true;
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const usage = value as Record<string, unknown>;
  const allowed = new Set([
    "total_tokens",
    "total_input_tokens",
    "input_tokens",
    "output_tokens",
    "thought_tokens",
    "cached_read_tokens",
    "cached_write_tokens",
    "used",
    "size",
  ]);
  return Object.entries(usage).every(
    ([key, item]) =>
      allowed.has(key) &&
      typeof item === "number" &&
      Number.isSafeInteger(item) &&
      item >= 0,
  );
}

function isJobError(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const failure = value as Record<string, unknown>;
  return (
    typeof failure.code === "string" &&
    typeof failure.message === "string" &&
    typeof failure.retryable === "boolean"
  );
}

function isStep(value: unknown): value is StepResult {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const step = value as Record<string, unknown>;
  if (
    typeof step.id !== "string" ||
    ![
      "pending",
      "running",
      "succeeded",
      "partial",
      "failed",
      "timed_out",
      "cancelled",
    ].includes(step.status as string) ||
    typeof step.started_at !== "string" ||
    typeof step.finished_at !== "string" ||
    typeof step.exit_code !== "number" ||
    !Number.isSafeInteger(step.exit_code) ||
    !(step.error === null || isJobError(step.error))
  )
    return false;
  if (Array.isArray(step.command)) {
    const allowed = new Set([
      "id",
      "command",
      "status",
      "started_at",
      "finished_at",
      "stdout",
      "stderr",
      "exit_code",
      "error",
    ]);
    return (
      Object.keys(step).every((key) => allowed.has(key)) &&
      step.status !== "partial" &&
      step.command.length >= 1 &&
      step.command.length <= 64 &&
      step.command.every(
        (argument) =>
          typeof argument === "string" &&
          encoder.encode(argument).byteLength <= 4096,
      ) &&
      typeof step.command[0] === "string" &&
      step.command[0].trim().length > 0 &&
      typeof step.stdout === "string" &&
      typeof step.stderr === "string"
    );
  }
  const allowed = new Set([
    "id",
    "prompt",
    "harness",
    "provider",
    "model",
    "reasoning_effort",
    "status",
    "started_at",
    "finished_at",
    "session_id",
    "stop_reason",
    "message",
    "usage",
    "exit_code",
    "error",
    "emitted_metrics",
  ]);
  return (
    Object.keys(step).every((key) => allowed.has(key)) &&
    typeof step.prompt === "string" &&
    typeof step.harness === "string" &&
    typeof step.provider === "string" &&
    typeof step.model === "string" &&
    (step.reasoning_effort === null ||
      ["none", "minimal", "low", "medium", "high", "xhigh", "max"].includes(
        step.reasoning_effort as string,
      )) &&
    (step.session_id === null || typeof step.session_id === "string") &&
    (step.stop_reason === null ||
      [
        "end_turn",
        "max_tokens",
        "max_turn_requests",
        "refusal",
        "cancelled",
      ].includes(step.stop_reason as string)) &&
    typeof step.message === "string" &&
    isUsage(step.usage) &&
    (step.emitted_metrics === undefined ||
      (Array.isArray(step.emitted_metrics) &&
        step.emitted_metrics.every(
          (name) =>
            typeof name === "string" &&
            /^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$/.test(name),
        )))
  );
}

/** Validates an untrusted completion payload returned by the Go supervisor. */
function isCompletion(value: unknown): value is RunnerCompletion {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  if (
    ![
      "running",
      "waiting_for_input",
      "budget_suspended",
      "succeeded",
      "partial",
      "failed",
      "timed_out",
      "cancelled",
    ].includes(item.status as string)
  )
    return false;
  if (
    typeof item.workflow !== "string" ||
    typeof item.workflow_digest !== "string" ||
    !Array.isArray(item.steps) ||
    !item.steps.every(isStep) ||
    !isUsage(item.usage) ||
    typeof item.event_sequence !== "number" ||
    !Number.isSafeInteger(item.event_sequence) ||
    item.event_sequence < 0
  )
    return false;
  if (!(item.error === null || isJobError(item.error))) return false;
  if (!(item.artifact_error === undefined || isJobError(item.artifact_error)))
    return false;
  if (!(item.memory_error === undefined || isJobError(item.memory_error)))
    return false;
  if (
    !(
      item.budget_reset_at === undefined ||
      (typeof item.budget_reset_at === "string" &&
        Number.isFinite(Date.parse(item.budget_reset_at)))
    )
  )
    return false;
  if (
    [
      "succeeded",
      "partial",
      "running",
      "waiting_for_input",
      "budget_suspended",
    ].includes(item.status as string) &&
    item.error !== null
  )
    return false;
  return true;
}

/** Adds only bounded workflow-step identifiers to a result-collection span. */
function setCurrentStepAttributes(
  span: TraceSpan,
  completion: RunnerCompletion,
): void {
  const step = completion.steps.find(
    (candidate) => candidate.status === "running",
  );
  if (!step) return;
  setSpanAttributes(span, {
    "agent_runner.step.id": step.id,
    "agent_runner.step.kind": "command" in step ? "command" : "agent",
    "agent_runner.harness.name": "harness" in step ? step.harness : undefined,
    "agent_runner.provider.name":
      "provider" in step ? step.provider : undefined,
    "agent_runner.model.name": "model" in step ? step.model : undefined,
    "agent_runner.reasoning_effort":
      "reasoning_effort" in step
        ? (step.reasoning_effort ?? undefined)
        : undefined,
  });
}

/** Compares two strings without returning early on the first differing byte. */
function safeEqual(left: string, right: string): boolean {
  let mismatch = left.length ^ right.length;
  for (let index = 0; index < left.length; index++) {
    mismatch |= left.charCodeAt(index) ^ (right.charCodeAt(index) || 0);
  }
  return mismatch === 0;
}

/** Reads a streaming body up to a strict byte limit, returning null on overflow. */
async function readLimitedBody(
  message: Request | Response,
  limit: number,
): Promise<Uint8Array | null> {
  if (!message.body) return new Uint8Array();
  const reader = message.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

/** Proxies container HTTPS traffic and injects credentials for matching rules. */
async function injectCredential(
  request: Request,
  env: Env,
  context: OutboundHandlerContext<EgressTraceContext>,
): Promise<Response> {
  const url = new URL(request.url);
  const target = credentialTarget(request);
  return traceAsync(
    tracing,
    spanNames.egressForward,
    {
      "http.request.method": request.method,
      "server.address": url.hostname,
      "agent_runner.job.id": context.params?.jobId,
      "agent_runner.workflow.name": context.params?.workflow,
      "flow.trace": true,
      "flow.trace.id": context.params?.traceId,
      "flow.run.id": context.params?.jobId,
      "agent_runner.egress.kind": target?.kind ?? "unmatched",
      "agent_runner.egress.target": target?.name,
    },
    async (span) => {
      const response = await injectCredentialRequest(
        request,
        env as unknown as Record<string, unknown>,
        context.containerId,
      );
      recordHttpResult(span, response);
      return response;
    },
  );
}

export class AgentContainer extends Container<Env> {
  defaultPort = appConfig.containerPort;
  requiredPorts = [appConfig.containerPort];
  enableInternet = true;
  interceptHttps = true;
  pingEndpoint = "ping";
  private eventFlush: Promise<void> | null = null;
  private interactionMessageUpdate: Promise<void> | null = null;
  private resumeAttempt: Promise<void> | null = null;
  private budgetResumeAttempt: Promise<BudgetResumeResult> | null = null;
  private readonly interactionWaiters = new Map<
    string,
    (response: InteractionResponse) => void
  >();

  /** Initializes the single-job state table owned by this container object. */
  constructor(ctx: DurableObjectState<{}>, env: Env) {
    super(ctx, env);
    this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS job (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        job_id TEXT NOT NULL,
        harness TEXT,
        workflow TEXT,
        workflow_version TEXT,
        workflow_default_harness TEXT,
        workflow_default_model TEXT,
        workflow_default_reasoning_effort TEXT,
        trigger_type TEXT NOT NULL DEFAULT 'api',
        trigger_schedule_id TEXT,
        trigger_cron TEXT,
        trigger_scheduled_at INTEGER,
        current_step TEXT,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        started_at INTEGER,
        finished_at INTEGER,
        expires_at INTEGER NOT NULL,
        callback_hash TEXT NOT NULL,
        session_id TEXT,
        stop_reason TEXT,
        result_message TEXT,
        usage_json TEXT,
        exit_code INTEGER,
        error_code TEXT,
        error_message TEXT,
        error_retryable INTEGER
        ,steps_json TEXT
        ,artifacts_json TEXT
        ,artifact_error_json TEXT
        ,memory_enabled INTEGER NOT NULL DEFAULT 0
        ,memory_key TEXT
        ,memory_source_etag TEXT
        ,memory_json TEXT
        ,memory_error_json TEXT
        ,finalization_deadline_at INTEGER
        ,event_received_batches INTEGER NOT NULL DEFAULT 0
        ,event_delivered_batches INTEGER NOT NULL DEFAULT 0
        ,event_last_error TEXT
        ,event_updated_at INTEGER
        ,workflow_deadline_at INTEGER
        ,remaining_workflow_ms INTEGER
        ,remaining_step_ms INTEGER
        ,budget_reset_at INTEGER
        ,budget_next_step INTEGER
        ,bundle_manifest_key TEXT
        ,callback_base_url TEXT
        ,checkpoint_key TEXT
        ,event_sequence INTEGER NOT NULL DEFAULT 3
        ,resume_attempts INTEGER NOT NULL DEFAULT 0
        ,trace_id TEXT NOT NULL
        ,run_span_id TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS event_outbox (
        id TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS interaction (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        request_json TEXT NOT NULL,
        response_json TEXT,
        created_at INTEGER NOT NULL,
        expires_at INTEGER NOT NULL,
        resolved_at INTEGER,
        provider_actor TEXT,
        provider_delivery_json TEXT,
        provider_message_updated_at INTEGER,
        provider_message_attempts INTEGER NOT NULL DEFAULT 0,
        provider_message_last_error TEXT
      );
    `);
    initializeHistory(this.ctx.storage.sql);
  }

  /** Persists a new job, stages its pinned workflow bundle, and starts the workflow. */
  async startJob(input: StartJobInput): Promise<JobView> {
    return traceAsync(
      tracing,
      spanNames.jobStart,
      {
        "agent_runner.job.id": input.jobId,
        "agent_runner.workflow.name": input.bundle.manifest.workflow.name,
        "agent_runner.workflow.version": input.bundle.manifest.digest,
        "flow.trace": true,
        "flow.trace.id": input.traceId,
        "flow.run.id": input.jobId,
      },
      (span) => this.startJobOperation(input, span),
    );
  }

  /** Coordinates the traced startup phases for one newly admitted job. */
  private async startJobOperation(
    input: StartJobInput,
    span: TraceSpan,
  ): Promise<JobView> {
    const existing = this.row();
    if (existing) {
      setJobAttributes(span, existing);
      span.setAttribute("agent_runner.outcome", "existing");
      return this.view(existing);
    }

    const callbackToken = randomToken();
    const callbackHash = await sha256(callbackToken);
    const expiresAt = input.createdAt + appConfig.retentionMs;
    let scheduledAt: number | null = null;
    if (input.trigger.type === "cron") {
      scheduledAt = Date.parse(input.trigger.scheduled_at);
      if (
        !Number.isSafeInteger(scheduledAt) ||
        scheduledAt <= 0 ||
        !/^[a-z][a-z0-9_-]{0,63}$/.test(input.trigger.schedule_id) ||
        input.trigger.cron.length > 256
      ) {
        throw new Error("job trigger is invalid");
      }
    }
    this.ctx.storage.sql.exec(
      `INSERT INTO job(singleton, job_id, harness, workflow, workflow_version, workflow_default_harness, workflow_default_model, workflow_default_reasoning_effort,
       trigger_type, trigger_schedule_id, trigger_cron, trigger_scheduled_at, status, created_at, expires_at, callback_hash, steps_json, trace_id, run_span_id, bundle_manifest_key, callback_base_url)
       VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'starting', ?, ?, ?, ?, ?, ?, ?, ?)`,
      input.jobId,
      input.bundle.manifest.workflow.name,
      input.bundle.manifest.workflow.name,
      input.bundle.manifest.digest,
      input.bundle.manifest.workflow.default_harness ?? null,
      input.bundle.manifest.workflow.default_model ?? null,
      input.bundle.manifest.workflow.default_reasoning_effort ?? null,
      input.trigger.type,
      input.trigger.type === "cron" ? input.trigger.schedule_id : null,
      input.trigger.type === "cron" ? input.trigger.cron : null,
      scheduledAt,
      input.createdAt,
      expiresAt,
      callbackHash,
      JSON.stringify(
        input.bundle.manifest.workflow.steps.map((step) =>
          "command" in step
            ? {
                ...step,
                status: "pending",
                started_at: "",
                finished_at: "",
                stdout: "",
                stderr: "",
                exit_code: 0,
                error: null,
              }
            : {
                ...step,
                status: "pending",
                started_at: "",
                finished_at: "",
                session_id: null,
                stop_reason: null,
                message: "",
                usage: null,
                exit_code: 0,
                error: null,
              },
        ),
      ),
      input.traceId,
      input.rootSpanId,
      workflowManifestKey(input.bundle.manifest),
      input.callbackBaseURL,
    );
    const memoryEnabled =
      input.bundle.manifest.workflow.memory_enabled === true;
    const memoryKey = memoryEnabled
      ? workflowMemoryKey(input.bundle.manifest.workflow.name)
      : null;
    this.ctx.storage.sql.exec(
      "UPDATE job SET memory_enabled = ?, memory_key = ? WHERE singleton = 1",
      Number(memoryEnabled),
      memoryKey,
    );
    await this.env.JOB_COORDINATOR.get(
      this.env.JOB_COORDINATOR.idFromName("global"),
    ).registerJob(input.jobId, input.createdAt, expiresAt);
    await this.schedule(new Date(expiresAt), "expireJob");
    if (historyEnabled) {
      this.addEventBatch(
        this.lifecycleBatch(
          input.jobId,
          input.createdAt,
          "starting",
          "run.created",
          0,
          { workflow_version: input.bundle.manifest.digest },
        ),
      );
      await this.requestEventOutboxFlush();
    }

    try {
      await this.setOutboundHandler<EgressTraceContext>("credentialProxy", {
        jobId: input.jobId,
        workflow: input.bundle.manifest.workflow.name,
        traceId: input.traceId,
      });
      const runnerSecret = (this.env as unknown as Record<string, string>)[
        appConfig.authSecretName
      ];
      if (!runnerSecret) throw new Error("runner API secret is not configured");
      const proxyToken = await containerProxyToken(
        runnerSecret,
        this.ctx.id.toString(),
      );
      const resolvedSteps = input.bundle.manifest.workflow.steps.map((step) => {
        if ("command" in step) return step;
        const model =
          appConfig.models[step.model as keyof typeof appConfig.models];
        const providerModels = model!.providers as Readonly<
          Record<string, string>
        >;
        return { ...step, model_id: providerModels[step.provider]! };
      });
      const workflowTimeoutMs =
        input.bundle.manifest.workflow.workflow_timeout_ms;
      const archive = await traceAsync(
        tracing,
        spanNames.workflowBundleLoad,
        {
          "agent_runner.job.id": input.jobId,
          "agent_runner.workflow.version": input.bundle.manifest.digest,
          "agent_runner.bundle.key": input.bundle.manifest.archive.key,
          "agent_runner.bundle.size": input.bundle.manifest.archive.size,
          "agent_runner.bundle.sha256": input.bundle.manifest.archive.sha256,
          "agent_runner.bundle.file_count": input.bundle.manifest.files.length,
          "agent_runner.bundle.total_bytes": input.bundle.manifest.total_bytes,
        },
        async (bundleSpan) => {
          setFlowTraceAttributes(bundleSpan, input.jobId, input.traceId);
          const value = await loadWorkflowBundleArchive(
            this.env,
            input.bundle.manifest,
          );
          bundleSpan.setAttribute("agent_runner.outcome", "success");
          return value;
        },
      );
      this.sleepAfter = containerSleepAfter(
        workflowTimeoutMs +
          (memoryEnabled ? appConfig.memoryLimits.persistenceTimeoutMs : 0),
      );
      this.renewActivityTimeout();
      await traceAsync(
        tracing,
        spanNames.containerBoot,
        {
          "agent_runner.job.id": input.jobId,
          "agent_runner.workflow.name": input.bundle.manifest.workflow.name,
        },
        async (bootSpan) => {
          await this.startAndWaitForPorts({
            ports: appConfig.containerPort,
            cancellationOptions: {
              instanceGetTimeoutMS: 30_000,
              portReadyTimeoutMS: 60_000,
            },
            startOptions: {
              enableInternet: true,
              labels: {
                job_id: input.jobId,
                workflow: input.bundle.manifest.workflow.name,
              },
              envVars: {
                ...runtimeEnvironment(
                  this.env as unknown as Record<string, unknown>,
                  appConfig.runtimeEnvironment,
                ),
                RUNNER_JOB_ID: input.jobId,
                RUNNER_EGRESS_TOKEN: proxyToken,
                RUNNER_CONFIG_JSON: JSON.stringify({
                  ...appConfig.runnerConfig,
                  workflow_timeout_ms: workflowTimeoutMs,
                  default_step_timeout_ms:
                    input.bundle.manifest.workflow.default_step_timeout_ms,
                  workflow: {
                    name: input.bundle.manifest.workflow.name,
                    bundle_digest: input.bundle.manifest.digest,
                    memory_enabled: memoryEnabled,
                    token_budget: input.bundle.manifest.workflow.token_budget,
                    steps: resolvedSteps,
                  },
                }),
              },
            },
          });
          bootSpan.setAttribute("agent_runner.outcome", "success");
        },
      );
      await traceAsync(
        tracing,
        spanNames.workflowStage,
        {
          "agent_runner.job.id": input.jobId,
          "agent_runner.workflow.version": input.bundle.manifest.digest,
          "agent_runner.bundle.key": input.bundle.manifest.archive.key,
          "agent_runner.bundle.size": input.bundle.manifest.archive.size,
          "agent_runner.bundle.sha256": input.bundle.manifest.archive.sha256,
          "agent_runner.bundle.file_count": input.bundle.manifest.files.length,
          "agent_runner.bundle.total_bytes": input.bundle.manifest.total_bytes,
        },
        async (stageSpan) => {
          setFlowTraceAttributes(stageSpan, input.jobId, input.traceId);
          const stagedManifest = await this.containerFetch(
            "http://container.internal/stage",
            {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(input.bundle.manifest),
            },
          );
          if (!stagedManifest.ok)
            throw new Error(
              `runner rejected workflow manifest with HTTP ${stagedManifest.status}: ${(await stagedManifest.text()).trim().slice(0, 4096)}`,
            );
          const stagedArchive = await this.containerFetch(
            "http://container.internal/stage/archive",
            {
              method: "PUT",
              headers: { "content-type": "application/gzip" },
              body: archive.buffer as ArrayBuffer,
            },
          );
          if (!stagedArchive.ok)
            throw new Error(
              `runner rejected workflow archive with HTTP ${stagedArchive.status}: ${(await stagedArchive.text()).trim().slice(0, 4096)}`,
            );
          stageSpan.setAttribute("agent_runner.outcome", "success");
        },
      );
      if (historyEnabled) {
        this.addEventBatch(
          this.lifecycleBatch(
            input.jobId,
            input.createdAt,
            "starting",
            "workflow.bundle.materialized",
            bundleMaterializedSequence,
            workflowBundleTelemetry(input.bundle.manifest),
          ),
        );
      }
      if (memoryEnabled) {
        const memoryMaterialized = await this.materializeMemory(memoryKey!);
        if (historyEnabled) {
          this.addEventBatch(
            this.lifecycleBatch(
              input.jobId,
              input.createdAt,
              "starting",
              "workflow.memory.materialized",
              memoryMaterializedSequence,
              memoryMaterialized,
            ),
          );
        }
      }
      const startedAt = Date.now();
      const deadlineAt = workflowDeadlineAt(startedAt, workflowTimeoutMs);
      this.ctx.storage.sql.exec(
        "UPDATE job SET started_at = ?, workflow_deadline_at = ? WHERE singleton = 1",
        startedAt,
        deadlineAt,
      );
      const response = await traceAsync(
        tracing,
        spanNames.workflowDispatch,
        {
          "agent_runner.job.id": input.jobId,
          "agent_runner.workflow.name": input.bundle.manifest.workflow.name,
        },
        async (dispatchSpan) => {
          const value = await this.containerFetch(
            "http://container.internal/run",
            {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({
                job_id: input.jobId,
                callback_token: callbackToken,
                callback_url: historyEnabled
                  ? `${input.callbackBaseURL}/internal/v1/jobs/${input.jobId}/events`
                  : "",
                interaction_url:
                  appConfig.interactions.provider === "slack"
                    ? `${input.callbackBaseURL}/internal/v1/jobs/${input.jobId}/interactions`
                    : "",
                budget_url: input.bundle.manifest.workflow.token_budget
                  ? `${input.callbackBaseURL}/internal/v1/jobs/${input.jobId}/token-usage`
                  : "",
                run_harness: flowRunHarness,
                run_created_at: new Date(input.createdAt).toISOString(),
                run_started_at: new Date(startedAt).toISOString(),
                trace_id: input.traceId,
                run_span_id: input.rootSpanId,
                deadline_at: new Date(deadlineAt).toISOString(),
              }),
            },
          );
          recordHttpResult(dispatchSpan, value);
          return value;
        },
      );
      if (response.status !== 202) {
        const detail = (await response.text()).trim().slice(0, 4096);
        throw new Error(
          `runner rejected job with HTTP ${response.status}${detail ? `: ${detail}` : ""}`,
        );
      }
      if (historyEnabled) {
        this.addEventBatch(
          this.lifecycleBatch(
            input.jobId,
            input.createdAt,
            "running",
            "run.started",
            runStartedSequence,
            { workflow_version: input.bundle.manifest.digest },
          ),
        );
        await this.requestEventOutboxFlush();
      }
      this.renewActivityTimeout();
      await this.schedule(1, "collectResult");
      span.setAttribute("agent_runner.job.status", "running");
      span.setAttribute("agent_runner.outcome", "success");
      return this.view(this.requireRow());
    } catch (error) {
      const memoryError =
        error instanceof MemoryOperationError
          ? {
              code: error.code,
              message: error.message,
              retryable: error.retryable,
            }
          : undefined;
      await this.finishWithError(
        "failed",
        memoryError?.code ?? "container_start_failed",
        errorMessage(error),
        memoryError?.retryable ?? true,
        memoryError,
      );
      await this.releaseCapacity(input.jobId);
      try {
        await this.stop();
      } catch {
        /* container may not have started */
      }
      throw error;
    }
  }

  getJob(): JobView | null {
    const row = this.row();
    return row && row.expires_at > Date.now() ? this.view(row) : null;
  }

  /** Atomically charges one completed agent step before the supervisor may continue. */
  async recordTokenUsage(
    token: string,
    payload: string,
  ): Promise<TokenBudgetCallbackResult> {
    const row = this.row();
    if (
      !row ||
      terminalStatuses.has(row.status) ||
      !safeEqual(await sha256(token), row.callback_hash)
    ) {
      return {
        accepted: false,
        status: 401,
        code: "unauthorized",
        message: "A valid callback token is required",
      };
    }
    let request: {
      step_id: string;
      step_index: number;
      tokens: number | null;
      completed_at: string;
      remaining_workflow_ms: number;
    };
    let stepCount = 0;
    try {
      request = JSON.parse(payload) as typeof request;
      const completedAt = Date.parse(request.completed_at);
      if (
        typeof request.step_id !== "string" ||
        !/^[a-z][a-z0-9_-]{0,63}$/.test(request.step_id) ||
        !Number.isSafeInteger(request.step_index) ||
        request.step_index < 0 ||
        !(
          request.tokens === null ||
          (Number.isSafeInteger(request.tokens) && request.tokens >= 0)
        ) ||
        !Number.isSafeInteger(completedAt) ||
        !Number.isSafeInteger(request.remaining_workflow_ms) ||
        request.remaining_workflow_ms < 1
      )
        throw new Error("invalid token usage charge");
      const steps = JSON.parse(row.steps_json ?? "[]") as StepResult[];
      if (steps[request.step_index]?.id !== request.step_id)
        throw new Error("token usage step does not match the running job");
      stepCount = steps.length;
    } catch (error) {
      return {
        accepted: false,
        status: 400,
        code: "invalid_token_usage",
        message: errorMessage(error),
      };
    }
    if (!row.bundle_manifest_key || !row.workflow_version || !row.workflow)
      return {
        accepted: false,
        status: 409,
        code: "workflow_state_missing",
        message: "Pinned workflow state is missing",
      };
    try {
      const bundle = await loadPinnedWorkflowBundle(
        this.env,
        row.bundle_manifest_key,
        row.workflow_version,
      );
      const policy = bundle.manifest.workflow.token_budget;
      if (!policy)
        return {
          accepted: false,
          status: 409,
          code: "token_budget_disabled",
          message: "The pinned workflow has no token budget",
        };
      const coordinator = this.env.WORKFLOW_BUDGET.get(
        this.env.WORKFLOW_BUDGET.idFromName(row.workflow),
      );
      const availability = await coordinator.charge({
        digest: bundle.manifest.digest,
        sortKey: bundle.manifest.sort_key,
        policy,
        jobId: row.job_id,
        stepIndex: request.step_index,
        tokens: request.tokens,
        completedAt: Date.parse(request.completed_at),
      });
      const action = availability.allowed ? "continue" : "suspend";
      if (
        action === "suspend" &&
        request.tokens !== null &&
        request.step_index + 1 < stepCount
      ) {
        const authoritativeRemaining =
          row.workflow_deadline_at === null
            ? (row.remaining_workflow_ms ?? request.remaining_workflow_ms)
            : Math.max(1, row.workflow_deadline_at - Date.now());
        this.ctx.storage.sql.exec(
          "UPDATE job SET workflow_deadline_at = NULL, remaining_workflow_ms = ?, budget_reset_at = ?, budget_next_step = ? WHERE singleton = 1",
          Math.min(authoritativeRemaining, request.remaining_workflow_ms),
          availability.resetAt,
          request.step_index + 1,
        );
      }
      return {
        accepted: true,
        action,
        ...(availability.resetAt
          ? { reset_at: new Date(availability.resetAt).toISOString() }
          : {}),
      };
    } catch (error) {
      return {
        accepted: false,
        status: 503,
        code: "budget_accounting_failed",
        message: errorMessage(error),
      };
    }
  }

  /** Persists an ACP form request and waits briefly for a provider response. */
  async requestInteraction(
    token: string,
    payload: string,
  ): Promise<InteractionCallbackResult> {
    const row = this.row();
    if (
      !row ||
      terminalStatuses.has(row.status) ||
      !safeEqual(await sha256(token), row.callback_hash)
    ) {
      return {
        accepted: false,
        status: 401,
        code: "unauthorized",
        message: "A valid callback token is required",
      };
    }
    if (appConfig.interactions.provider !== "slack") {
      return {
        accepted: false,
        status: 409,
        code: "interactions_disabled",
        message: "Human input is not configured",
      };
    }
    let request: ElicitationRequest;
    let stepId: string;
    let remainingStepMs: number;
    try {
      const envelope = JSON.parse(payload) as Record<string, unknown>;
      if (
        typeof envelope.step_id !== "string" ||
        !/^[a-z][a-z0-9_-]{0,63}$/.test(envelope.step_id) ||
        typeof envelope.remaining_step_ms !== "number" ||
        !Number.isSafeInteger(envelope.remaining_step_ms) ||
        envelope.remaining_step_ms < 1
      )
        throw new Error("interaction timing is invalid");
      request = parseElicitationRequest(envelope.request);
      stepId = envelope.step_id;
      remainingStepMs = envelope.remaining_step_ms;
    } catch (error) {
      return {
        accepted: false,
        status: 400,
        code: "invalid_elicitation",
        message: errorMessage(error),
      };
    }
    const pending = this.pendingInteraction();
    if (pending)
      return {
        accepted: true,
        state: "resolved",
        response: { action: "cancel" },
      };
    const id = crypto.randomUUID();
    const now = Date.now();
    const expiresAt = now + appConfig.interactions.responseTtlMs;
    this.ctx.storage.sql.exec(
      "INSERT INTO interaction(id, status, request_json, created_at, expires_at) VALUES (?, 'pending', ?, ?, ?)",
      id,
      JSON.stringify(request),
      now,
      expiresAt,
    );
    await this.schedule(new Date(expiresAt), "expireInteraction");
    const remaining =
      row.workflow_deadline_at === null
        ? null
        : Math.max(1, row.workflow_deadline_at - now);
    const jobExpiresAt = Math.max(row.expires_at, expiresAt);
    this.ctx.storage.sql.exec(
      "UPDATE job SET status = 'waiting_for_input', current_step = ?, workflow_deadline_at = NULL, remaining_workflow_ms = ?, remaining_step_ms = ?, expires_at = ? WHERE singleton = 1",
      stepId,
      remaining,
      remainingStepMs,
      jobExpiresAt,
    );
    if (jobExpiresAt !== row.expires_at) {
      await this.env.JOB_COORDINATOR.get(
        this.env.JOB_COORDINATOR.idFromName("global"),
      ).registerJob(row.job_id, row.created_at, jobExpiresAt);
      await this.schedule(new Date(jobExpiresAt), "expireJob");
    }
    let timer: ReturnType<typeof setTimeout>;
    const responsePromise = new Promise<InteractionResponse | null>(
      (resolve) => {
        timer = setTimeout(
          () => resolve(null),
          appConfig.interactions.liveWaitMs,
        );
        this.interactionWaiters.set(id, (value) => {
          clearTimeout(timer);
          resolve(value);
        });
      },
    );
    try {
      const delivery = await deliverInteraction(
        this.env,
        { job_id: row.job_id, interaction_id: id },
        request.message,
        { workflow: row.workflow ?? "unknown", stepId },
      );
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET provider_delivery_json = ? WHERE id = ?",
        JSON.stringify(delivery),
        id,
      );
      if (this.interaction(id)?.status !== "pending")
        await this.requestInteractionMessageUpdate();
    } catch (error) {
      clearTimeout(timer!);
      this.interactionWaiters.delete(id);
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET status = 'resolved', response_json = ?, resolved_at = ? WHERE id = ?",
        JSON.stringify({ action: "cancel" }),
        Date.now(),
        id,
      );
      this.resumeDeadlineAndStatus("running");
      return {
        accepted: false,
        status: 502,
        code: "interaction_delivery_failed",
        message: errorMessage(error),
      };
    }
    const response = await responsePromise;
    this.interactionWaiters.delete(id);
    if (response) {
      if (interactionContinuesCurrentPrompt(response))
        this.resumeDeadlineAndStatus("running");
      return {
        accepted: true,
        state: "resolved",
        interaction_id: id,
        response,
      };
    }
    return { accepted: true, state: "pending", interaction_id: id };
  }

  /** Returns a reason-coded interaction state without exposing persisted response content. */
  getInteractionForProvider(
    interactionId: string,
    userId: string,
  ): InteractionProviderLookup {
    if (!interactionActorAllowed(userId)) return { state: "forbidden" };
    const row = this.interaction(interactionId);
    if (!row) return { state: "missing" };
    if (row.status === "resolved") return { state: "resolved" };
    if (row.status === "expired" || row.expires_at <= Date.now())
      return { state: "expired" };
    if (row.status !== "pending") return { state: "missing" };
    try {
      return {
        state: "pending",
        request: JSON.parse(row.request_json) as ElicitationRequest,
      };
    } catch {
      return { state: "missing" };
    }
  }

  /** Atomically accepts the first valid provider answer for a pending form. */
  async resolveInteractionFromProvider(
    interactionId: string,
    userId: string,
    response: InteractionResponse,
  ): Promise<InteractionProviderResolution> {
    const interactionConfig = appConfig.interactions;
    if (
      interactionConfig.provider !== "slack" ||
      !interactionActorAllowed(userId)
    )
      return { accepted: false, state: "forbidden" };
    const interaction = this.interaction(interactionId);
    if (!interaction) return { accepted: false, state: "missing" };
    if (interaction.status === "resolved")
      return { accepted: false, state: "resolved" };
    if (
      interaction.status === "expired" ||
      interaction.expires_at <= Date.now()
    )
      return { accepted: false, state: "expired" };
    if (interaction.status !== "pending")
      return { accepted: false, state: "missing" };
    if (!(["accept", "decline", "cancel"] as const).includes(response.action))
      return {
        accepted: false,
        state: "pending",
        validationError: "Invalid response action",
      };
    const request = JSON.parse(interaction.request_json) as ElicitationRequest;
    if (response.action === "accept") {
      if (
        !response.content ||
        typeof response.content !== "object" ||
        Array.isArray(response.content)
      )
        return {
          accepted: false,
          state: "pending",
          validationError: "Response content is required",
        };
      const validationError = validateElicitationContent(
        request.requestedSchema,
        response.content,
      );
      if (validationError)
        return { accepted: false, state: "pending", validationError };
    }
    const encoded = JSON.stringify(response);
    if (encoder.encode(encoded).byteLength > interactionConfig.maxResponseBytes)
      return {
        accepted: false,
        state: "pending",
        validationError: "Response is too large",
      };
    this.ctx.storage.sql.exec(
      "UPDATE interaction SET status = 'resolved', response_json = ?, resolved_at = ?, provider_actor = ? WHERE id = ? AND status = 'pending'",
      encoded,
      Date.now(),
      userId,
      interactionId,
    );
    await this.requestInteractionMessageUpdate();
    const waiter = this.interactionWaiters.get(interactionId);
    if (waiter) waiter(response);
    else {
      const job = this.requireRow();
      if (
        job.checkpoint_key &&
        (job.status === "sleeping" || job.status === "waiting_for_input")
      ) {
        await this.transitionPausedJob(
          "resuming",
          "run.resuming",
          job.event_sequence + 1,
          { interaction_id: interactionId, step_id: job.current_step },
        );
        await this.schedule(1, "resumeJob");
      } else {
        this.ctx.storage.sql.exec(
          "UPDATE job SET status = 'resuming' WHERE singleton = 1 AND status = 'waiting_for_input'",
        );
      }
    }
    return { accepted: true, state: "resolved" };
  }

  private interaction(id: string): InteractionRow | null {
    return (
      this.ctx.storage.sql
        .exec<InteractionRow>("SELECT * FROM interaction WHERE id = ?", id)
        .toArray()[0] ?? null
    );
  }

  private pendingInteraction(): InteractionRow | null {
    return (
      this.ctx.storage.sql
        .exec<InteractionRow>(
          "SELECT * FROM interaction WHERE status = 'pending' ORDER BY created_at LIMIT 1",
        )
        .toArray()[0] ?? null
    );
  }

  private resumeDeadlineAndStatus(status: "running" | "resuming"): void {
    const row = this.requireRow();
    const deadline =
      row.remaining_workflow_ms === null
        ? row.workflow_deadline_at
        : Date.now() + row.remaining_workflow_ms;
    this.ctx.storage.sql.exec(
      "UPDATE job SET status = ?, workflow_deadline_at = ?, remaining_workflow_ms = NULL WHERE singleton = 1",
      status,
      deadline,
    );
  }

  /** Persists and exports one sequence-ordered cold interaction transition. */
  private async transitionPausedJob(
    status: "sleeping" | "budget_suspended" | "resuming",
    type: "run.sleeping" | "run.budget_suspended" | "run.resuming",
    sequence: number,
    data: object,
  ): Promise<void> {
    const row = this.requireRow();
    this.ctx.storage.sql.exec(
      "UPDATE job SET status = ?, event_sequence = ? WHERE singleton = 1",
      status,
      sequence,
    );
    if (!historyEnabled) return;
    this.addEventBatch(
      this.lifecycleBatch(
        row.job_id,
        row.created_at,
        status,
        type,
        sequence,
        data,
      ),
    );
    await this.requestEventOutboxFlush();
  }

  /** Delivers terminal interaction message updates without delaying provider callbacks. */
  async updateInteractionMessages(): Promise<void> {
    if (this.interactionMessageUpdate) return this.interactionMessageUpdate;
    this.interactionMessageUpdate =
      this.deliverInteractionMessageUpdate().finally(() => {
        this.interactionMessageUpdate = null;
      });
    return this.interactionMessageUpdate;
  }

  /** Updates one persisted provider message and retains transient failures for retry. */
  private async deliverInteractionMessageUpdate(): Promise<void> {
    const interaction = this.ctx.storage.sql
      .exec<InteractionRow>(
        `SELECT * FROM interaction
       WHERE status IN ('resolved', 'expired')
         AND provider_delivery_json IS NOT NULL
         AND provider_message_updated_at IS NULL
         AND provider_message_attempts < 5
       ORDER BY resolved_at, created_at
       LIMIT 1`,
      )
      .toArray()[0];
    if (!interaction) return;
    const delivery = this.interactionDelivery(interaction);
    const outcome = this.interactionOutcome(interaction);
    if (!delivery || !outcome) {
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET provider_message_attempts = 5, provider_message_last_error = ? WHERE id = ?",
        "Persisted interaction delivery metadata is invalid",
        interaction.id,
      );
      this.logInteractionMessageFailure(
        interaction,
        5,
        "Persisted interaction delivery metadata is invalid",
      );
      if (this.hasPendingInteractionMessageUpdate())
        await this.schedule(1, "updateInteractionMessages");
      return;
    }
    try {
      const request = JSON.parse(
        interaction.request_json,
      ) as ElicitationRequest;
      await completeInteraction(this.env, delivery, request.message, outcome);
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET provider_message_updated_at = ?, provider_message_last_error = NULL WHERE id = ?",
        Date.now(),
        interaction.id,
      );
      if (this.hasPendingInteractionMessageUpdate())
        await this.schedule(1, "updateInteractionMessages");
    } catch (error) {
      const attempts = interaction.provider_message_attempts + 1;
      const message = errorMessage(error).slice(0, 1024);
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET provider_message_attempts = ?, provider_message_last_error = ? WHERE id = ?",
        attempts,
        message,
        interaction.id,
      );
      this.logInteractionMessageFailure(interaction, attempts, message);
      if (attempts < 5) {
        const requestedDelay = interactionRetryAfter(error) ?? 0;
        const backoff = Math.min(300, 2 ** Math.min(attempts, 8));
        await this.schedule(
          Math.max(requestedDelay, backoff),
          "updateInteractionMessages",
        );
      } else if (this.hasPendingInteractionMessageUpdate()) {
        await this.schedule(1, "updateInteractionMessages");
      }
    }
  }

  /** Restores a validated provider reference from durable interaction state. */
  private interactionDelivery(
    interaction: InteractionRow,
  ): InteractionDelivery | null {
    if (!interaction.provider_delivery_json) return null;
    try {
      return parseInteractionDelivery(
        JSON.parse(interaction.provider_delivery_json),
      );
    } catch {
      return null;
    }
  }

  /** Maps durable interaction state to the provider-neutral terminal display state. */
  private interactionOutcome(
    interaction: InteractionRow,
  ): InteractionOutcome | null {
    if (interaction.status === "expired") return { status: "expired" };
    if (!interaction.response_json) return null;
    let response: InteractionResponse;
    try {
      response = JSON.parse(interaction.response_json) as InteractionResponse;
    } catch {
      return null;
    }
    if (!interaction.provider_actor)
      return response.action === "cancel" ? { status: "run_cancelled" } : null;
    const resolvedAt = interaction.resolved_at ?? interaction.created_at;
    switch (response.action) {
      case "accept":
        return {
          status: "answered",
          actorId: interaction.provider_actor,
          resolvedAt,
        };
      case "decline":
        return {
          status: "declined",
          actorId: interaction.provider_actor,
          resolvedAt,
        };
      case "cancel":
        return {
          status: "cancelled",
          actorId: interaction.provider_actor,
          resolvedAt,
        };
    }
  }

  /** Reports whether any terminal interaction still needs its provider message updated. */
  private hasPendingInteractionMessageUpdate(): boolean {
    return (
      this.ctx.storage.sql
        .exec<{ count: number }>(
          `SELECT COUNT(*) AS count FROM interaction
       WHERE status IN ('resolved', 'expired')
         AND provider_delivery_json IS NOT NULL
         AND provider_message_updated_at IS NULL
         AND provider_message_attempts < 5`,
        )
        .one().count > 0
    );
  }

  /** Persists a retry before starting a best-effort immediate provider update. */
  private async requestInteractionMessageUpdate(): Promise<void> {
    if (!this.hasPendingInteractionMessageUpdate()) return;
    await this.schedule(1, "updateInteractionMessages");
    this.ctx.waitUntil(this.updateInteractionMessages());
  }

  /** Records provider update failures without exposing response content. */
  private logInteractionMessageFailure(
    interaction: InteractionRow,
    attempts: number,
    message: string,
  ): void {
    const row = this.row();
    console.error(
      JSON.stringify({
        level: "error",
        source: "worker",
        job_id: row?.job_id,
        workflow: row?.workflow,
        trace_id: row?.trace_id,
        span_id: row?.run_span_id,
        interaction_id: interaction.id,
        event: "interaction_message_update_failed",
        attempts,
        error: message,
      }),
    );
  }

  /** Handles a late response after the original container has been checkpointed. */
  async resumeJob(): Promise<void> {
    if (this.resumeAttempt) return this.resumeAttempt;
    this.resumeAttempt = this.resumeJobOperation().finally(() => {
      this.resumeAttempt = null;
    });
    return this.resumeAttempt;
  }

  /** Restores a step-boundary checkpoint after the shared workflow quota becomes available. */
  async resumeBudgetJob(): Promise<{
    state: "accepted" | "deferred" | "gone";
  }> {
    if (this.budgetResumeAttempt) return this.budgetResumeAttempt;
    this.budgetResumeAttempt = this.resumeBudgetJobOperation().finally(() => {
      this.budgetResumeAttempt = null;
    });
    return this.budgetResumeAttempt;
  }

  /** Performs one single-flight token-budget resume attempt. */
  private async resumeBudgetJobOperation(): Promise<BudgetResumeResult> {
    const row = this.row();
    if (!row || row.status !== "budget_suspended") return { state: "gone" };
    if (
      !row.checkpoint_key ||
      !row.bundle_manifest_key ||
      !row.workflow_version ||
      !row.callback_base_url ||
      row.started_at === null ||
      row.remaining_workflow_ms === null ||
      row.remaining_workflow_ms < 1 ||
      row.budget_next_step === null ||
      row.budget_next_step < 0
    ) {
      await this.finishResumeFailure(
        row,
        "budget_checkpoint_invalid",
        "Saved budget suspension state is incomplete",
        false,
      );
      return { state: "gone" };
    }
    if (!shouldRetryResume(row.resume_attempts, true)) {
      await this.finishResumeFailure(
        row,
        "budget_resume_failed",
        "Cold resume retry budget was exhausted",
        true,
      );
      return { state: "gone" };
    }
    const coordinator = this.env.JOB_COORDINATOR.get(
      this.env.JOB_COORDINATOR.idFromName("global"),
    );
    const capacity = await coordinator.acquire(
      row.job_id,
      null,
      "budget-resume",
      Date.now(),
      row.remaining_workflow_ms,
    );
    if (capacity.kind === "full") return { state: "deferred" };
    const remaining = row.remaining_workflow_ms;
    const attempt = row.resume_attempts + 1;
    let dispatchAccepted = false;
    try {
      this.ctx.storage.sql.exec(recordResumeAttemptSQL, attempt);
      const bundle = await loadPinnedWorkflowBundle(
        this.env,
        row.bundle_manifest_key,
        row.workflow_version,
      );
      const checkpointObject = await this.env.RUNNER_STORAGE.get(
        row.checkpoint_key,
      );
      if (
        !checkpointObject ||
        checkpointObject.size > appConfig.checkpointLimits.maxTotalBytes
      ) {
        throw new ResumeOperationError(
          "Saved budget checkpoint is missing or too large",
          false,
        );
      }
      const [archive, checkpoint] = await Promise.all([
        loadWorkflowBundleArchive(this.env, bundle.manifest),
        checkpointObject.arrayBuffer().then((value) => new Uint8Array(value)),
      ]);
      const steps = JSON.parse(row.steps_json ?? "null") as StepResult[];
      if (
        !Array.isArray(steps) ||
        steps.length !== bundle.manifest.workflow.steps.length
      )
        throw new ResumeOperationError(
          "Saved workflow steps are invalid",
          false,
        );
      const callbackToken = randomToken();
      const callbackHash = await sha256(callbackToken);
      const runnerSecret = (this.env as unknown as Record<string, string>)[
        appConfig.authSecretName
      ];
      if (!runnerSecret) throw new Error("runner API secret is not configured");
      const proxyToken = await containerProxyToken(
        runnerSecret,
        this.ctx.id.toString(),
      );
      await this.setOutboundHandler<EgressTraceContext>("credentialProxy", {
        jobId: row.job_id,
        workflow: row.workflow ?? undefined,
        traceId: row.trace_id,
      });
      const resolvedSteps = bundle.manifest.workflow.steps.map((step) => {
        if ("command" in step) return step;
        const model =
          appConfig.models[step.model as keyof typeof appConfig.models];
        return {
          ...step,
          model_id: (model!.providers as Readonly<Record<string, string>>)[
            step.provider
          ]!,
        };
      });
      this.sleepAfter = containerSleepAfter(
        remaining +
          (row.memory_enabled
            ? appConfig.memoryLimits.persistenceTimeoutMs
            : 0),
      );
      await this.startAndWaitForPorts({
        ports: appConfig.containerPort,
        cancellationOptions: {
          instanceGetTimeoutMS: 30_000,
          portReadyTimeoutMS: 60_000,
        },
        startOptions: {
          enableInternet: true,
          labels: { job_id: row.job_id, workflow: row.workflow ?? "workflow" },
          envVars: {
            ...runtimeEnvironment(
              this.env as unknown as Record<string, unknown>,
              appConfig.runtimeEnvironment,
            ),
            RUNNER_JOB_ID: row.job_id,
            RUNNER_EGRESS_TOKEN: proxyToken,
            RUNNER_CONFIG_JSON: JSON.stringify({
              ...appConfig.runnerConfig,
              workflow_timeout_ms: remaining,
              default_step_timeout_ms:
                bundle.manifest.workflow.default_step_timeout_ms,
              workflow: {
                name: bundle.manifest.workflow.name,
                bundle_digest: bundle.manifest.digest,
                memory_enabled: row.memory_enabled === 1,
                token_budget: bundle.manifest.workflow.token_budget,
                steps: resolvedSteps,
              },
            }),
          },
        },
      });
      const manifestResponse = await this.containerFetch(
        "http://container.internal/stage",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(bundle.manifest),
        },
      );
      if (!manifestResponse.ok)
        throw await this.resumeResponseError(
          "workflow manifest",
          manifestResponse,
        );
      const archiveResponse = await this.containerFetch(
        "http://container.internal/stage/archive",
        {
          method: "PUT",
          headers: { "content-type": "application/gzip" },
          body: archive.buffer as ArrayBuffer,
        },
      );
      if (!archiveResponse.ok)
        throw await this.resumeResponseError(
          "workflow archive",
          archiveResponse,
        );
      const checkpointResponse = await this.containerFetch(
        "http://container.internal/checkpoint",
        {
          method: "PUT",
          headers: { "content-type": "application/gzip" },
          body: checkpoint.buffer as ArrayBuffer,
        },
      );
      if (!checkpointResponse.ok)
        throw await this.resumeResponseError("checkpoint", checkpointResponse);
      if (row.memory_enabled && row.memory_key)
        await this.materializeMemory(row.memory_key);

      const resumedAt = Date.now();
      const deadlineAt = resumedAt + remaining;
      this.ctx.storage.sql.exec(
        "UPDATE job SET workflow_deadline_at = ?, remaining_workflow_ms = NULL, budget_reset_at = NULL, budget_next_step = NULL WHERE singleton = 1 AND status = 'budget_suspended'",
        deadlineAt,
      );
      await this.transitionPausedJob(
        "resuming",
        "run.resuming",
        row.event_sequence + 1,
        { reason: "token_budget", step_index: row.budget_next_step },
      );
      const resumeSequence = this.requireRow().event_sequence;
      this.ctx.storage.sql.exec(prepareResumeDispatchSQL, callbackHash);
      const dispatch = await this.containerFetch(
        "http://container.internal/run",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            job_id: row.job_id,
            callback_token: callbackToken,
            callback_url: historyEnabled
              ? `${row.callback_base_url}/internal/v1/jobs/${row.job_id}/events`
              : "",
            interaction_url:
              appConfig.interactions.provider === "slack"
                ? `${row.callback_base_url}/internal/v1/jobs/${row.job_id}/interactions`
                : "",
            budget_url: bundle.manifest.workflow.token_budget
              ? `${row.callback_base_url}/internal/v1/jobs/${row.job_id}/token-usage`
              : "",
            run_harness: flowRunHarness,
            run_created_at: new Date(row.created_at).toISOString(),
            run_started_at: new Date(row.started_at).toISOString(),
            trace_id: row.trace_id,
            run_span_id: row.run_span_id,
            deadline_at: new Date(deadlineAt).toISOString(),
            resume: {
              kind: "budget",
              step_index: row.budget_next_step,
              steps,
              usage: row.usage_json
                ? (JSON.parse(row.usage_json) as SessionUsage)
                : null,
              event_sequence: resumeSequence,
              remaining_step_ms: 0,
            },
          }),
        },
      );
      if (dispatch.status !== 202)
        throw await this.resumeResponseError("budget-resumed job", dispatch);
      dispatchAccepted = true;
      this.ctx.storage.sql.exec(
        "UPDATE job SET status = 'running', memory_json=NULL, memory_error_json=NULL WHERE singleton = 1 AND status = 'resuming' AND workflow_deadline_at IS NOT NULL",
      );
      this.ctx.waitUntil(
        this.cleanupResumeCheckpoint(row.job_id, row.checkpoint_key),
      );
      await this.schedule(1, "collectResult");
      return { state: "accepted" };
    } catch (error) {
      if (dispatchAccepted) {
        console.error(
          JSON.stringify({
            level: "error",
            source: "worker",
            job_id: row.job_id,
            event: "budget_resume_monitor_failed",
            attempt,
            error: errorMessage(error),
          }),
        );
        this.ctx.waitUntil(this.collectResult());
        return { state: "accepted" };
      }
      await this.releaseCapacity(row.job_id);
      try {
        await this.destroy();
      } catch {
        /* container may already be stopped */
      }
      this.ctx.storage.sql.exec(
        "UPDATE job SET workflow_deadline_at = NULL, remaining_workflow_ms = ?, budget_reset_at = ?, budget_next_step = ? WHERE singleton = 1 AND status = 'resuming'",
        remaining,
        row.budget_reset_at,
        row.budget_next_step,
      );
      const retryable =
        !(error instanceof ResumeOperationError) || error.retryable;
      if (!shouldRetryResume(attempt, retryable)) {
        await this.finishResumeFailure(
          this.requireRow(),
          "budget_resume_failed",
          errorMessage(error),
          retryable,
        );
        return { state: "gone" };
      }
      const latest = this.requireRow();
      await this.transitionPausedJob(
        "budget_suspended",
        "run.budget_suspended",
        latest.event_sequence + 1,
        {
          reset_at:
            latest.budget_reset_at === null
              ? null
              : new Date(latest.budget_reset_at).toISOString(),
          attempt,
          retrying: true,
          error: { code: "budget_resume_failed", retryable: true },
        },
      );
      console.error(
        JSON.stringify({
          level: "error",
          source: "worker",
          job_id: row.job_id,
          event: "budget_resume_deferred",
          attempt,
          error: errorMessage(error),
        }),
      );
      return { state: "deferred" };
    }
  }

  /** Performs one single-flight cold-resume attempt. */
  private async resumeJobOperation(): Promise<void> {
    const row = this.row();
    if (!row || row.status !== "resuming") return;
    const remaining = row.remaining_workflow_ms;
    const stateProblem = resumeStateProblem({
      checkpointKey: row.checkpoint_key,
      sessionId: row.session_id,
      bundleManifestKey: row.bundle_manifest_key,
      workflowVersion: row.workflow_version,
      callbackBaseUrl: row.callback_base_url,
      startedAt: row.started_at,
      remainingWorkflowMs: remaining,
      remainingStepMs: row.remaining_step_ms,
    });
    if (stateProblem) {
      await this.finishResumeFailure(
        row,
        "interaction_checkpoint_invalid",
        stateProblem,
        false,
      );
      return;
    }
    if (!shouldRetryResume(row.resume_attempts, true)) {
      await this.finishResumeFailure(
        row,
        "interaction_resume_failed",
        "Cold resume retry budget was exhausted",
        true,
      );
      return;
    }
    const coordinator = this.env.JOB_COORDINATOR.get(
      this.env.JOB_COORDINATOR.idFromName("global"),
    );
    const capacity = await coordinator.acquire(
      row.job_id,
      null,
      "interaction-resume",
      Date.now(),
      remaining!,
    );
    if (capacity.kind === "full") {
      await this.schedule(10, "resumeJob");
      return;
    }
    const attempt = row.resume_attempts + 1;
    let interactionId: string | null = null;
    let dispatchAccepted = false;
    try {
      this.ctx.storage.sql.exec(recordResumeAttemptSQL, attempt);
      const bundle = await loadPinnedWorkflowBundle(
        this.env,
        row.bundle_manifest_key!,
        row.workflow_version!,
      );
      const archive = await loadWorkflowBundleArchive(
        this.env,
        bundle.manifest,
      );
      const checkpointObject = await this.env.RUNNER_STORAGE.get(
        row.checkpoint_key!,
      );
      if (
        !checkpointObject ||
        appConfig.interactions.provider !== "slack" ||
        checkpointObject.size >
          appConfig.interactions.checkpointLimits.maxTotalBytes
      )
        throw new ResumeOperationError(
          "Saved checkpoint is missing or too large",
          false,
        );
      const checkpoint = new Uint8Array(await checkpointObject.arrayBuffer());
      const interaction = this.ctx.storage.sql
        .exec<InteractionRow>(
          "SELECT * FROM interaction WHERE status = 'resolved' ORDER BY resolved_at DESC LIMIT 1",
        )
        .toArray()[0];
      if (!interaction?.response_json)
        throw new ResumeOperationError(
          "Resolved interaction response is missing",
          false,
        );
      interactionId = interaction.id;
      let steps: StepResult[];
      let response: InteractionResponse;
      try {
        const parsedSteps = JSON.parse(row.steps_json ?? "null") as unknown;
        if (!Array.isArray(parsedSteps))
          throw new Error("steps are not an array");
        steps = parsedSteps as StepResult[];
        response = JSON.parse(interaction.response_json) as InteractionResponse;
      } catch {
        throw new ResumeOperationError(
          "Saved interaction or workflow state is invalid",
          false,
        );
      }
      const stepIndex = steps.findIndex(
        (step) => step.status === "running" && !("command" in step),
      );
      if (stepIndex < 0)
        throw new ResumeOperationError(
          "Paused workflow step is missing",
          false,
        );

      const callbackToken = randomToken();
      const callbackHash = await sha256(callbackToken);
      const runnerSecret = (this.env as unknown as Record<string, string>)[
        appConfig.authSecretName
      ];
      if (!runnerSecret) throw new Error("runner API secret is not configured");
      const proxyToken = await containerProxyToken(
        runnerSecret,
        this.ctx.id.toString(),
      );
      await this.setOutboundHandler<EgressTraceContext>("credentialProxy", {
        jobId: row.job_id,
        workflow: row.workflow ?? undefined,
        traceId: row.trace_id,
      });
      const resolvedSteps = bundle.manifest.workflow.steps.map((step) => {
        if ("command" in step) return step;
        const model =
          appConfig.models[step.model as keyof typeof appConfig.models];
        return {
          ...step,
          model_id: (model!.providers as Readonly<Record<string, string>>)[
            step.provider
          ]!,
        };
      });
      this.sleepAfter = containerSleepAfter(
        remaining! +
          (row.memory_enabled
            ? appConfig.memoryLimits.persistenceTimeoutMs
            : 0),
      );
      await this.startAndWaitForPorts({
        ports: appConfig.containerPort,
        cancellationOptions: {
          instanceGetTimeoutMS: 30_000,
          portReadyTimeoutMS: 60_000,
        },
        startOptions: {
          enableInternet: true,
          labels: { job_id: row.job_id, workflow: row.workflow ?? "workflow" },
          envVars: {
            ...runtimeEnvironment(
              this.env as unknown as Record<string, unknown>,
              appConfig.runtimeEnvironment,
            ),
            RUNNER_JOB_ID: row.job_id,
            RUNNER_EGRESS_TOKEN: proxyToken,
            RUNNER_CONFIG_JSON: JSON.stringify({
              ...appConfig.runnerConfig,
              workflow_timeout_ms: remaining!,
              default_step_timeout_ms:
                bundle.manifest.workflow.default_step_timeout_ms,
              workflow: {
                name: bundle.manifest.workflow.name,
                bundle_digest: bundle.manifest.digest,
                memory_enabled: row.memory_enabled === 1,
                token_budget: bundle.manifest.workflow.token_budget,
                steps: resolvedSteps,
              },
            }),
          },
        },
      });
      const manifestResponse = await this.containerFetch(
        "http://container.internal/stage",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(bundle.manifest),
        },
      );
      if (!manifestResponse.ok)
        throw await this.resumeResponseError(
          "workflow manifest",
          manifestResponse,
        );
      const archiveResponse = await this.containerFetch(
        "http://container.internal/stage/archive",
        {
          method: "PUT",
          headers: { "content-type": "application/gzip" },
          body: archive.buffer as ArrayBuffer,
        },
      );
      if (!archiveResponse.ok)
        throw await this.resumeResponseError(
          "workflow archive",
          archiveResponse,
        );
      const checkpointResponse = await this.containerFetch(
        "http://container.internal/checkpoint",
        {
          method: "PUT",
          headers: { "content-type": "application/gzip" },
          body: checkpoint.buffer as ArrayBuffer,
        },
      );
      if (!checkpointResponse.ok)
        throw await this.resumeResponseError("checkpoint", checkpointResponse);
      if (row.memory_enabled && row.memory_key)
        await this.materializeMemory(row.memory_key);

      const resumedAt = Date.now();
      const deadlineAt = resumedAt + remaining!;
      // Authenticate callbacks emitted during ACP setup without committing the
      // saved budget until the supervisor acknowledges the resumed session.
      this.ctx.storage.sql.exec(prepareResumeDispatchSQL, callbackHash);
      const dispatch = await this.containerFetch(
        "http://container.internal/run",
        {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(
            buildResumeRunRequest({
              jobId: row.job_id,
              callbackToken,
              callbackBaseUrl: row.callback_base_url!,
              historyEnabled: Boolean(historyEnabled),
              budgetEnabled: Boolean(bundle.manifest.workflow.token_budget),
              runCreatedAt: row.created_at,
              runStartedAt: row.started_at!,
              traceId: row.trace_id,
              runSpanId: row.run_span_id,
              deadlineAt,
              stepIndex,
              sessionId: row.session_id!,
              response,
              steps,
              eventSequence: row.event_sequence,
              remainingStepMs: row.remaining_step_ms!,
            }),
          ),
        },
      );
      if (dispatch.status !== 202)
        throw await this.resumeResponseError("resumed job", dispatch);
      if (this.requireRow().status !== "resuming") {
        await this.releaseCapacity(row.job_id);
        try {
          await this.destroy();
        } catch {
          /* container may already be stopped */
        }
        return;
      }
      this.ctx.storage.sql.exec(acceptResumeDispatchSQL, deadlineAt);
      this.ctx.storage.sql.exec(
        "UPDATE job SET memory_json=NULL, memory_error_json=NULL WHERE singleton=1",
      );
      dispatchAccepted = true;
      this.ctx.waitUntil(
        this.cleanupResumeCheckpoint(row.job_id, row.checkpoint_key!),
      );
      await this.schedule(1, "collectResult");
    } catch (error) {
      if (dispatchAccepted) {
        console.error(
          JSON.stringify({
            level: "error",
            source: "worker",
            job_id: row.job_id,
            event: "interaction_resume_monitor_failed",
            attempt,
            error: errorMessage(error),
          }),
        );
        this.ctx.waitUntil(this.collectResult());
        return;
      }
      await this.releaseCapacity(row.job_id);
      console.error(
        JSON.stringify({
          level: "error",
          source: "worker",
          job_id: row.job_id,
          event: "interaction_resume_failed",
          attempt,
          error: errorMessage(error),
        }),
      );
      try {
        await this.destroy();
      } catch {
        /* container may not have started */
      }
      const current = this.row();
      if (!current || terminalStatuses.has(current.status)) return;
      const retryable =
        !(error instanceof ResumeOperationError) || error.retryable;
      if (!shouldRetryResume(attempt, retryable)) {
        await this.finishResumeFailure(
          current,
          "interaction_resume_failed",
          errorMessage(error),
          retryable,
        );
        return;
      }
      const latest = this.requireRow();
      await this.transitionPausedJob(
        "resuming",
        "run.resuming",
        latest.event_sequence + 1,
        {
          interaction_id: interactionId,
          step_id: latest.current_step,
          attempt,
          retrying: true,
          error: { code: "interaction_resume_failed", retryable: true },
        },
      );
      await this.schedule(10, "resumeJob");
    }
  }

  /** Finalizes an unrecoverable resume and removes its no-longer-usable checkpoint. */
  private async finishResumeFailure(
    row: JobRow,
    code: string,
    message: string,
    retryable: boolean,
  ): Promise<void> {
    await this.finishWithError("failed", code, message, retryable);
    if (row.checkpoint_key)
      await this.cleanupResumeCheckpoint(row.job_id, row.checkpoint_key);
  }

  /** Deletes one accepted or terminal resume checkpoint without changing job outcome. */
  private async cleanupResumeCheckpoint(
    jobId: string,
    checkpointKey: string,
  ): Promise<void> {
    try {
      await this.env.RUNNER_STORAGE.delete(checkpointKey);
      this.ctx.storage.sql.exec(
        "UPDATE job SET checkpoint_key = NULL WHERE singleton = 1 AND checkpoint_key = ?",
        checkpointKey,
      );
    } catch (error) {
      console.error(
        JSON.stringify({
          level: "error",
          source: "worker",
          job_id: jobId,
          event: "interaction_checkpoint_cleanup_failed",
          error: errorMessage(error),
        }),
      );
    }
  }

  /** Converts a bounded runner rejection into a classified resume failure. */
  private async resumeResponseError(
    operation: string,
    response: Response,
  ): Promise<ResumeOperationError> {
    const bytes = await readLimitedBody(response, 4096);
    const detail =
      bytes && bytes.byteLength > 0
        ? new TextDecoder().decode(bytes).trim()
        : "";
    const retryable =
      response.status === 408 ||
      response.status === 409 ||
      response.status === 425 ||
      response.status === 429 ||
      response.status >= 500;
    const suffix = detail ? `: ${detail}` : "";
    return new ResumeOperationError(
      `runner rejected ${operation} with HTTP ${response.status}${suffix}`,
      retryable,
    );
  }

  /** Expires an unanswered interaction without keeping a container active. */
  async expireInteraction(): Promise<void> {
    const interaction = this.pendingInteraction();
    if (!interaction || interaction.expires_at > Date.now()) return;
    this.ctx.storage.sql.exec(
      "UPDATE interaction SET status = 'expired', resolved_at = ? WHERE id = ? AND status = 'pending'",
      Date.now(),
      interaction.id,
    );
    await this.requestInteractionMessageUpdate();
    const row = this.row();
    if (
      row &&
      (row.status === "waiting_for_input" ||
        row.status === "sleeping" ||
        row.status === "resuming")
    ) {
      await this.finishWithError(
        "timed_out",
        "interaction_timeout",
        "The pending interaction expired before a response was received",
        false,
      );
      await this.releaseCapacity(row.job_id);
      if (row.checkpoint_key)
        await this.env.RUNNER_STORAGE.delete(row.checkpoint_key);
    }
  }

  /** Polls the Go supervisor until a validated terminal result is available. */
  async collectResult(): Promise<void> {
    return traceAsync(tracing, spanNames.resultCollect, {}, (span) =>
      this.collectResultOperation(span),
    );
  }

  /** Performs one bounded supervisor polling and finalization pass. */
  private async collectResultOperation(span: TraceSpan): Promise<void> {
    const row = this.row();
    setJobAttributes(span, row);
    if (!row || terminalStatuses.has(row.status)) {
      span.setAttribute("agent_runner.outcome", "noop");
      return;
    }
    try {
      const response = await this.containerFetch(
        "http://container.internal/result",
      );
      if (response.status === 202) {
        const bytes = await readLimitedBody(
          response,
          appConfig.maxResultBytes * 6 + 256_000,
        );
        if (bytes && bytes.byteLength > 0) {
          const progress: unknown = JSON.parse(new TextDecoder().decode(bytes));
          if (
            isCompletion(progress) &&
            progress.status === "running" &&
            progress.workflow_digest === row.workflow_version
          ) {
            this.persistProgress(progress);
            setCurrentStepAttributes(span, progress);
          }
        }
        if (this.workflowTimedOut(row) && row.status !== "cancelling") {
          await this.finishTimedOutWorkflow();
          return;
        }
        const current = this.requireRow();
        if (
          current.status === "cancelling" &&
          current.finalization_deadline_at !== null &&
          Date.now() >= current.finalization_deadline_at
        ) {
          await this.finishExpiredCancellation(current);
          span.setAttribute("agent_runner.outcome", "timeout");
          return;
        }
        span.setAttribute("agent_runner.result.state", "running");
        span.setAttribute("agent_runner.outcome", "pending");
        this.renewActivityTimeout();
        await this.schedule(1, "collectResult");
        return;
      }
      if (response.status !== 200)
        throw new Error(`result endpoint returned HTTP ${response.status}`);
      const bytes = await readLimitedBody(
        response,
        appConfig.maxResultBytes * 6 + 65_536,
      );
      if (!bytes) throw new Error("result payload is too large");
      const completion: unknown = JSON.parse(new TextDecoder().decode(bytes));
      if (
        !isCompletion(completion) ||
        completion.status === "running" ||
        completion.workflow_digest !== row.workflow_version ||
        encoder.encode(
          JSON.stringify(
            completion.steps.map((step) =>
              "command" in step ? [step.stdout, step.stderr] : step.message,
            ),
          ),
        ).byteLength > appConfig.maxResultBytes
      ) {
        throw new Error("result payload is invalid");
      }
      if (completion.status === "budget_suspended") {
        const resetAt = completion.budget_reset_at
          ? Date.parse(completion.budget_reset_at)
          : NaN;
        if (
          !Number.isSafeInteger(resetAt) ||
          resetAt < 0 ||
          row.remaining_workflow_ms === null ||
          row.budget_next_step === null
        ) {
          throw new Error("budget suspension state is invalid");
        }
        const checkpointResponse = await this.containerFetch(
          "http://container.internal/checkpoint",
        );
        if (!checkpointResponse.ok)
          throw new Error(
            `checkpoint endpoint returned HTTP ${checkpointResponse.status}`,
          );
        const checkpoint = await readLimitedBody(
          checkpointResponse,
          appConfig.checkpointLimits.maxTotalBytes,
        );
        if (!checkpoint) throw new Error("checkpoint is too large");
        const key = jobCheckpointKey(row.job_id);
        await this.env.RUNNER_STORAGE.put(key, checkpoint, {
          httpMetadata: { contentType: "application/gzip" },
        });
        let memory = row.memory_json
          ? (JSON.parse(row.memory_json) as MemoryMetadata)
          : undefined;
        if (row.memory_enabled) memory = await this.persistMemorySnapshot(row);
        const expiresAt = Math.max(
          row.expires_at,
          resetAt + appConfig.retentionMs,
        );
        this.ctx.storage.sql.exec(
          `UPDATE job SET checkpoint_key = ?, steps_json = ?, usage_json = ?, memory_json = ?, status = 'budget_suspended',
             current_step = NULL, budget_reset_at = ?, expires_at = ?, resume_attempts = 0, event_sequence = ? WHERE singleton = 1`,
          key,
          JSON.stringify(completion.steps),
          completion.usage ? JSON.stringify(completion.usage) : null,
          memory ? JSON.stringify(memory) : null,
          resetAt,
          expiresAt,
          completion.event_sequence,
        );
        await this.env.JOB_COORDINATOR.get(
          this.env.JOB_COORDINATOR.idFromName("global"),
        ).registerJob(row.job_id, row.created_at, expiresAt);
        await this.schedule(new Date(expiresAt), "expireJob");
        await this.releaseCapacity(row.job_id);
        await this.stop();
        await this.transitionPausedJob(
          "budget_suspended",
          "run.budget_suspended",
          completion.event_sequence + 1,
          {
            reset_at: new Date(resetAt).toISOString(),
          },
        );
        const coordinator = this.env.WORKFLOW_BUDGET.get(
          this.env.WORKFLOW_BUDGET.idFromName(row.workflow!),
        );
        await coordinator.suspend(row.job_id, Date.now());
        span.setAttribute("agent_runner.outcome", "budget_suspended");
        return;
      }
      if (completion.status === "waiting_for_input") {
        const checkpointResponse = await this.containerFetch(
          "http://container.internal/checkpoint",
        );
        if (!checkpointResponse.ok)
          throw new Error(
            `checkpoint endpoint returned HTTP ${checkpointResponse.status}`,
          );
        const limit =
          appConfig.interactions.provider === "slack"
            ? appConfig.interactions.checkpointLimits.maxTotalBytes
            : 1;
        const checkpoint = await readLimitedBody(checkpointResponse, limit);
        if (!checkpoint) throw new Error("checkpoint is too large");
        const key = jobCheckpointKey(row.job_id);
        await this.env.RUNNER_STORAGE.put(key, checkpoint, {
          httpMetadata: { contentType: "application/gzip" },
        });
        let memory = row.memory_json
          ? (JSON.parse(row.memory_json) as MemoryMetadata)
          : undefined;
        if (row.memory_enabled) {
          memory = await this.persistMemorySnapshot(row);
        }
        const resolved = this.ctx.storage.sql
          .exec<InteractionRow>(
            "SELECT * FROM interaction WHERE status = 'resolved' ORDER BY resolved_at DESC LIMIT 1",
          )
          .toArray()[0];
        const pausedStep = completion.steps.find(
          (step) => step.status === "running" && "session_id" in step,
        );
        const pausedSessionId =
          pausedStep && "session_id" in pausedStep
            ? pausedStep.session_id
            : null;
        this.ctx.storage.sql.exec(
          "UPDATE job SET checkpoint_key = ?, steps_json = ?, usage_json = ?, session_id = ?, memory_json = ?, resume_attempts = 0 WHERE singleton = 1",
          key,
          JSON.stringify(completion.steps),
          completion.usage ? JSON.stringify(completion.usage) : null,
          pausedSessionId,
          memory ? JSON.stringify(memory) : null,
        );
        const coldStatus = resolved ? "resuming" : "sleeping";
        const lifecycleSequence =
          Math.max(
            completion.event_sequence,
            this.requireRow().event_sequence,
          ) + 1;
        await this.transitionPausedJob(
          coldStatus,
          resolved ? "run.resuming" : "run.sleeping",
          lifecycleSequence,
          {
            interaction_id:
              resolved?.id ?? this.pendingInteraction()?.id ?? null,
            step_id: row.current_step,
          },
        );
        await this.releaseCapacity(row.job_id);
        await this.stop();
        if (resolved) await this.schedule(1, "resumeJob");
        span.setAttribute("agent_runner.outcome", "waiting_for_input");
        return;
      }
      setCurrentStepAttributes(span, completion);
      span.setAttribute("agent_runner.result.state", "terminal");
      span.setAttribute("agent_runner.job.status", completion.status);
      const finalizationDeadline =
        row.finalization_deadline_at ??
        Date.now() + appConfig.memoryLimits.persistenceTimeoutMs;
      this.ctx.storage.sql.exec(
        "UPDATE job SET status = 'finalizing', current_step = NULL, steps_json = ?, usage_json = ?, finalization_deadline_at = ? WHERE singleton = 1",
        JSON.stringify(completion.steps),
        completion.usage ? JSON.stringify(completion.usage) : null,
        finalizationDeadline,
      );
      let memory = row.memory_json
        ? (JSON.parse(row.memory_json) as MemoryMetadata)
        : undefined;
      let memoryError = completion.memory_error;
      if (row.memory_enabled && !memory && !memoryError) {
        try {
          memory = await this.persistMemorySnapshot(this.requireRow());
          this.ctx.storage.sql.exec(
            "UPDATE job SET memory_json = ? WHERE singleton = 1",
            JSON.stringify(memory),
          );
        } catch (error) {
          const failure =
            error instanceof MemoryOperationError
              ? error
              : new MemoryOperationError(
                  "memory_persist_failed",
                  errorMessage(error),
                  true,
                );
          if (failure.retryable && Date.now() < finalizationDeadline) {
            span.setAttribute("agent_runner.outcome", "retry");
            await this.schedule(2, "collectResult");
            return;
          }
          memoryError = {
            code: failure.code,
            message: failure.message,
            retryable: failure.retryable,
          };
        }
      }
      let finalCompletion: RunnerCompletion =
        memoryError &&
        (completion.status === "succeeded" || completion.status === "partial")
          ? {
              ...completion,
              status: "failed",
              error: memoryError,
              memory_error: memoryError,
            }
          : {
              ...completion,
              ...(memoryError ? { memory_error: memoryError } : {}),
            };
      let artifacts: ArtifactMetadata[] = [];
      if (!finalCompletion.artifact_error) {
        try {
          artifacts = await this.uploadArtifacts(row.job_id);
        } catch (error) {
          if (Date.now() < finalizationDeadline) {
            await this.schedule(2, "collectResult");
            return;
          }
          const artifactError = {
            code: "artifact_persist_failed",
            message: errorMessage(error),
            retryable: true,
          };
          finalCompletion = {
            ...finalCompletion,
            artifact_error: artifactError,
            ...(["succeeded", "partial"].includes(finalCompletion.status)
              ? { status: "failed" as const, error: artifactError }
              : {}),
          };
        }
      }
      await this.persistCompletion(
        finalCompletion,
        artifacts,
        memory,
        memoryError,
      );
      await this.stop();
      span.setAttribute("agent_runner.outcome", "success");
    } catch (error) {
      span.setAttribute("error.type", errorType(error));
      console.error(
        JSON.stringify({
          level: "error",
          source: "worker",
          job_id: row.job_id,
          workflow: row.workflow,
          trace_id: row.trace_id,
          span_id: row.run_span_id,
          event: "result_collection_failed",
          error: errorMessage(error),
        }),
      );
      const current = this.row();
      if (
        current?.status === "finalizing" &&
        current.memory_enabled &&
        !current.memory_json &&
        current.finalization_deadline_at !== null &&
        Date.now() >= current.finalization_deadline_at
      ) {
        const memoryError = {
          code: "memory_persist_failed",
          message:
            "Memory could not be persisted before the finalization deadline",
          retryable: true,
        };
        await this.finishWithError(
          "failed",
          memoryError.code,
          memoryError.message,
          memoryError.retryable,
          memoryError,
        );
        await this.releaseCapacity(current.job_id);
        await this.stop("SIGTERM");
        span.setAttribute("agent_runner.outcome", "timeout");
      } else if (
        current?.status === "cancelling" &&
        current.finalization_deadline_at !== null &&
        Date.now() >= current.finalization_deadline_at
      ) {
        span.setAttribute("agent_runner.outcome", "timeout");
        await this.finishExpiredCancellation(current);
      } else if (
        current &&
        !terminalStatuses.has(current.status) &&
        this.workflowTimedOut(current) &&
        current.status !== "finalizing"
      ) {
        span.setAttribute("agent_runner.outcome", "timeout");
        await this.finishTimedOutWorkflow();
      } else if (current && !terminalStatuses.has(current.status)) {
        span.setAttribute("agent_runner.outcome", "retry");
        await this.schedule(2, "collectResult");
      } else {
        span.setAttribute("agent_runner.outcome", "error");
      }
    }
  }

  /** Returns whether the configured aggregate workflow deadline has elapsed. */
  private workflowTimedOut(row: JobRow): boolean {
    return workflowDeadlineElapsed(row.workflow_deadline_at);
  }

  /** Ends a cancellation that could not produce a safe terminal snapshot in time. */
  private async finishExpiredCancellation(row: JobRow): Promise<void> {
    const timedOut = this.workflowTimedOut(row);
    const memoryError = row.memory_enabled
      ? {
          code: "memory_persist_failed",
          message: "Workflow did not stop in time to snapshot memory",
          retryable: true,
        }
      : undefined;
    await this.finishWithError(
      timedOut ? "timed_out" : "cancelled",
      timedOut ? "workflow_timeout" : "cancelled",
      timedOut
        ? "Workflow exceeded the configured aggregate timeout"
        : "Workflow cancellation did not complete before the finalization deadline",
      false,
      memoryError,
    );
    await this.releaseCapacity(row.job_id);
    await this.stop("SIGTERM");
  }

  /** Cancels a timed-out workflow while retaining the container for bounded finalization. */
  private async finishTimedOutWorkflow(): Promise<void> {
    const row = this.requireRow();
    try {
      await this.containerFetch("http://container.internal/cancel", {
        method: "POST",
      });
    } catch {
      /* stopping the container below remains authoritative */
    }
    if (!row.memory_enabled) {
      await this.finishWithError(
        "timed_out",
        "workflow_timeout",
        "Workflow exceeded the configured aggregate timeout",
        false,
      );
      await this.releaseCapacity(row.job_id);
      await this.stop("SIGTERM");
      return;
    }
    this.ctx.storage.sql.exec(
      "UPDATE job SET status = 'cancelling', finalization_deadline_at = COALESCE(finalization_deadline_at, ?) WHERE singleton = 1",
      Date.now() + appConfig.memoryLimits.persistenceTimeoutMs,
    );
    this.renewActivityTimeout();
    await this.schedule(1, "collectResult");
  }

  private persistProgress(progress: RunnerCompletion): void {
    const current =
      progress.steps.find((step) => step.status === "running")?.id ?? null;
    this.ctx.storage.sql.exec(
      "UPDATE job SET current_step = ?, steps_json = ?, usage_json = ? WHERE singleton = 1",
      current,
      JSON.stringify(progress.steps),
      progress.usage ? JSON.stringify(progress.usage) : null,
    );
  }

  /** Restores the workflow database, or creates its first valid empty snapshot. */
  private async materializeMemory(
    key: string,
  ): Promise<MemoryMaterializationMetadata> {
    return traceAsync(
      tracing,
      spanNames.memoryLoad,
      {
        "agent_runner.memory.key": key,
      },
      async (span) => {
        const row = this.requireRow();
        setJobAttributes(span, row);
        const object = await this.env.RUNNER_STORAGE.get(key);
        if (
          object &&
          (object.size < 1 ||
            object.size > appConfig.memoryLimits.maxDatabaseBytes)
        ) {
          throw new MemoryOperationError(
            "memory_load_failed",
            "Stored memory database exceeds the configured size limit",
            false,
          );
        }
        const response = object
          ? await this.containerFetch("http://container.internal/memory", {
              method: "PUT",
              headers: { "content-type": "application/vnd.sqlite3" },
              body: object.body,
            })
          : await this.containerFetch(
              "http://container.internal/memory/initialize",
              { method: "POST" },
            );
        if (!response.ok) {
          const detail = (await response.text()).trim().slice(0, 4096);
          throw new MemoryOperationError(
            "memory_load_failed",
            `runner rejected ${object ? "stored" : "new"} memory with HTTP ${response.status}${detail ? `: ${detail}` : ""}`,
            false,
          );
        }
        this.ctx.storage.sql.exec(
          "UPDATE job SET memory_source_etag = ? WHERE singleton = 1",
          object?.etag ?? null,
        );
        span.setAttribute("agent_runner.memory.existed", object !== null);
        span.setAttribute(
          "agent_runner.memory.source",
          object ? "r2" : "initialized",
        );
        span.setAttribute("agent_runner.memory.size", object?.size ?? 0);
        if (object) {
          span.setAttribute("agent_runner.memory.etag", object.etag);
          const objectDigest = object.customMetadata?.sha256;
          if (objectDigest && /^[0-9a-f]{64}$/.test(objectDigest))
            span.setAttribute("agent_runner.memory.sha256", objectDigest);
        }
        span.setAttribute("agent_runner.outcome", "success");
        return {
          key,
          source: object ? "r2" : "initialized",
          object: object
            ? {
                size: object.size,
                etag: object.etag,
                sha256:
                  object.customMetadata?.sha256 &&
                  /^[0-9a-f]{64}$/.test(object.customMetadata.sha256)
                    ? object.customMetadata.sha256
                    : null,
                persisted_at: object.uploaded.toISOString(),
              }
            : null,
        };
      },
    );
  }

  /** Atomically replaces the canonical R2 object with the checked supervisor snapshot. */
  private async persistMemorySnapshot(row: JobRow): Promise<MemoryMetadata> {
    if (!row.memory_key)
      throw new MemoryOperationError(
        "memory_persist_failed",
        "Memory key is missing",
        false,
      );
    const key = row.memory_key;
    return traceAsync(
      tracing,
      spanNames.memoryPersist,
      {
        "agent_runner.job.id": row.job_id,
        "agent_runner.memory.key": key,
      },
      async (span) => {
        setJobAttributes(span, row);
        const response = await this.containerFetch(
          "http://container.internal/memory",
        );
        if (!response.ok || !response.body) {
          const detail = (await response.text()).trim().slice(0, 4096);
          throw new MemoryOperationError(
            response.status === 422
              ? "memory_snapshot_failed"
              : "memory_persist_failed",
            `memory snapshot returned HTTP ${response.status}${detail ? `: ${detail}` : ""}`,
            response.status !== 422,
          );
        }
        const size = Number(response.headers.get("content-length"));
        const digest = response.headers.get("x-content-sha256") ?? "";
        if (
          !Number.isSafeInteger(size) ||
          size < 1 ||
          size > appConfig.memoryLimits.maxDatabaseBytes ||
          !/^[0-9a-f]{64}$/.test(digest)
        ) {
          throw new MemoryOperationError(
            "memory_snapshot_failed",
            "Memory snapshot metadata is invalid",
            false,
          );
        }
        let stored: R2Object | null;
        try {
          stored = await writeFixedLengthBody(
            response.body,
            size,
            (body) =>
              this.env.RUNNER_STORAGE.put(key, body, {
                onlyIf: row.memory_source_etag
                  ? { etagMatches: row.memory_source_etag }
                  : { etagDoesNotMatch: "*" },
                httpMetadata: { contentType: "application/vnd.sqlite3" },
                customMetadata: { job_id: row.job_id, sha256: digest },
                sha256: hexadecimalBytes(digest),
              }),
            (result) => result !== null,
          );
        } catch (error) {
          throw new MemoryOperationError(
            "memory_persist_failed",
            `Could not write memory database: ${errorMessage(error)}`,
            true,
          );
        }
        if (!stored) {
          const existing = await this.env.RUNNER_STORAGE.head(key);
          if (
            existing?.customMetadata?.job_id === row.job_id &&
            existing.customMetadata.sha256 === digest &&
            existing.size === size
          ) {
            stored = existing;
          } else {
            throw new MemoryOperationError(
              "memory_conflict",
              "Memory database changed after this job loaded it",
              false,
            );
          }
        }
        if (stored.size !== size)
          throw new MemoryOperationError(
            "memory_persist_failed",
            "Stored memory database has an unexpected size",
            true,
          );
        this.ctx.storage.sql.exec(
          "UPDATE job SET memory_source_etag=? WHERE singleton=1",
          stored.etag,
        );
        const metadata = {
          key,
          size,
          sha256: digest,
          etag: stored.etag,
          persisted_at: stored.uploaded.toISOString(),
        };
        span.setAttribute("agent_runner.memory.size", size);
        span.setAttribute("agent_runner.memory.sha256", digest);
        span.setAttribute("agent_runner.outcome", "success");
        return metadata;
      },
    );
  }

  /** Streams the supervisor's bounded output files into the job's R2 prefix. */
  private async uploadArtifacts(jobId: string): Promise<ArtifactMetadata[]> {
    return traceAsync(
      tracing,
      spanNames.artifactsUpload,
      {
        "agent_runner.job.id": jobId,
      },
      (span) => this.uploadArtifactsOperation(jobId, span),
    );
  }

  /** Validates and persists artifacts exposed by the supervisor. */
  private async uploadArtifactsOperation(
    jobId: string,
    span: TraceSpan,
  ): Promise<ArtifactMetadata[]> {
    const response = await this.containerFetch(
      "http://container.internal/artifacts",
    );
    if (!response.ok)
      throw new Error(`artifact manifest returned HTTP ${response.status}`);
    const value: unknown = await response.json();
    if (!value || typeof value !== "object" || Array.isArray(value))
      throw new Error("artifact manifest is invalid");
    const manifestArtifacts = (value as { artifacts?: unknown }).artifacts;
    if (manifestArtifacts === null) {
      span.setAttribute("agent_runner.artifact.count", 0);
      span.setAttribute("agent_runner.outcome", "success");
      return [];
    }
    if (!Array.isArray(manifestArtifacts))
      throw new Error("artifact manifest is invalid");
    const rawArtifacts = manifestArtifacts;
    if (rawArtifacts.length > appConfig.artifactLimits.maxFiles)
      throw new Error("artifact manifest exceeds maxFiles");
    const artifacts: ArtifactMetadata[] = [];
    let total = 0;
    for (const [position, raw] of rawArtifacts.entries()) {
      if (!raw || typeof raw !== "object" || Array.isArray(raw))
        throw new Error("artifact entry is invalid");
      const item = raw as Record<string, unknown>;
      if (
        item.index !== position ||
        typeof item.path !== "string" ||
        !safeArtifactPath(item.path) ||
        typeof item.size !== "number" ||
        !Number.isSafeInteger(item.size) ||
        item.size < 0 ||
        item.size > appConfig.artifactLimits.maxFileBytes ||
        typeof item.sha256 !== "string" ||
        !/^[0-9a-f]{64}$/.test(item.sha256) ||
        typeof item.content_type !== "string"
      )
        throw new Error("artifact entry is invalid");
      total += item.size;
      if (total > appConfig.artifactLimits.maxTotalBytes)
        throw new Error("artifacts exceed maxTotalBytes");
      const fileResponse = await this.containerFetch(
        `http://container.internal/artifacts/${position}`,
      );
      if (!fileResponse.ok)
        throw new Error(
          `artifact ${item.path} returned HTTP ${fileResponse.status}`,
        );
      const bytes = await readLimitedBody(
        fileResponse,
        appConfig.artifactLimits.maxFileBytes,
      );
      if (
        !bytes ||
        bytes.byteLength !== item.size ||
        (await sha256Bytes(bytes)) !== item.sha256
      )
        throw new Error(`artifact ${item.path} failed integrity validation`);
      const key = jobOutputKey(jobId, item.path);
      const stored = await this.env.RUNNER_STORAGE.put(key, bytes, {
        httpMetadata: { contentType: item.content_type },
      });
      artifacts.push({
        path: item.path,
        key,
        size: item.size,
        sha256: item.sha256,
        etag: stored.httpEtag,
        content_type: item.content_type,
      });
    }
    span.setAttribute("agent_runner.artifact.count", artifacts.length);
    span.setAttribute("agent_runner.outcome", "success");
    return artifacts;
  }

  /** Accepts a token-authenticated history callback from the supervisor. */
  async enqueueEvents(
    token: string,
    payload: string,
  ): Promise<{ accepted: boolean; status?: number; message?: string }> {
    return traceAsync(tracing, spanNames.flowEnqueue, {}, async (span) => {
      const result = await this.enqueueEventsOperation(token, payload, span);
      span.setAttribute(
        "agent_runner.outcome",
        result.accepted ? "success" : "rejected",
      );
      return result;
    });
  }

  /** Authenticates and persists one supervisor event batch. */
  private async enqueueEventsOperation(
    token: string,
    payload: string,
    span: TraceSpan,
  ): Promise<{ accepted: boolean; status?: number; message?: string }> {
    const row = this.row();
    setJobAttributes(span, row);
    if (
      !row ||
      row.expires_at <= Date.now() ||
      !safeEqual(await sha256(token), row.callback_hash)
    )
      return { accepted: false };
    const metadata = flowBatchMetadata(payload, row);
    if (!metadata)
      return {
        accepted: false,
        status: 400,
        message: "Invalid history batch or trace context",
      };
    try {
      this.addEventBatch(payload, false);
    } catch (error) {
      if (error instanceof HistoryError)
        return {
          accepted: false,
          status: error.status,
          message: error.message,
        };
      throw error;
    }
    if (metadata.maxSequence !== null) {
      this.ctx.storage.sql.exec(
        recordReceivedEventSequenceSQL,
        metadata.maxSequence,
      );
    }
    await this.requestEventOutboxFlush();
    return { accepted: true };
  }

  /** Starts at most one outbox delivery pass for concurrent callbacks and alarms. */
  async flushEventOutbox(): Promise<void> {
    if (this.eventFlush) return this.eventFlush;
    this.eventFlush = traceAsync(tracing, spanNames.flowDeliver, {}, (span) => {
      setJobAttributes(span, this.row());
      return this.deliverEventOutbox(span);
    }).finally(() => {
      this.eventFlush = null;
    });
    return this.eventFlush;
  }

  /** Archives persisted event batches in R2, retaining failures for retry. */
  private async deliverEventOutbox(span: TraceSpan): Promise<void> {
    if (!historyEnabled) {
      span.setAttribute("agent_runner.outcome", "disabled");
      return;
    }
    const batches = this.ctx.storage.sql
      .exec<OutboxRow>(
        "SELECT id, payload, attempts FROM event_outbox ORDER BY created_at LIMIT 10",
      )
      .toArray();
    span.setAttribute("agent_runner.batch.count", batches.length);
    const row = this.requireRow();
    if (!batches.length) {
      span.setAttribute("agent_runner.outcome", "noop");
      return;
    }
    for (const batch of batches) {
      try {
        await archiveHistory(
          this.env.RUNNER_STORAGE,
          this.ctx.storage.sql,
          row.job_id,
          batch.payload,
        );
        this.ctx.storage.sql.exec(
          "DELETE FROM event_outbox WHERE id = ?",
          batch.id,
        );
        this.ctx.storage.sql.exec(recordDeliveredEventBatchSQL, Date.now());
        console.log(
          JSON.stringify({
            level: "info",
            source: "worker",
            job_id: row.job_id,
            workflow: row.workflow,
            trace_id: row.trace_id,
            span_id: row.run_span_id,
            event: "history_archived",
          }),
        );
      } catch (error) {
        this.ctx.storage.sql.exec(
          "UPDATE event_outbox SET attempts = attempts + 1 WHERE id = ?",
          batch.id,
        );
        this.recordDeliveryError(errorMessage(error));
        console.error(
          JSON.stringify({
            level: "error",
            source: "worker",
            job_id: row.job_id,
            workflow: row.workflow,
            trace_id: row.trace_id,
            span_id: row.run_span_id,
            event: "history_archive_failed",
            attempts: batch.attempts + 1,
            error: errorMessage(error),
          }),
        );
        await this.schedule(
          Math.min(300, 2 ** Math.min(batch.attempts + 1, 8)),
          "flushEventOutbox",
        );
        span.setAttribute("agent_runner.outcome", "retry");
        span.setAttribute("error.type", errorType(error));
        return;
      }
    }
    const pending = this.pendingEventBatches();
    if (pending > 0) await this.schedule(1, "flushEventOutbox");
    span.setAttribute("agent_runner.outcome", "success");
  }

  /** Permanently removes an undeliverable Flow batch and records the rejection. */

  /** Atomically stores a validated completion and releases global capacity. */
  private async persistCompletion(
    completion: RunnerCompletion,
    artifacts: ArtifactMetadata[],
    memory?: MemoryMetadata,
    memoryError?: JobError,
  ): Promise<void> {
    return traceAsync(
      tracing,
      spanNames.jobFinalize,
      {
        "agent_runner.workflow.name": completion.workflow,
        "agent_runner.workflow.version": completion.workflow_digest,
        "agent_runner.job.status": completion.status,
        "agent_runner.artifact.count": artifacts.length,
      },
      (span) => {
        const row = this.requireRow();
        setJobAttributes(span, row);
        span.setAttribute("agent_runner.job.status", completion.status);
        return this.persistCompletionOperation(
          completion,
          artifacts,
          memory,
          memoryError,
          span,
          row,
        );
      },
    );
  }

  /** Stores one terminal result and its lifecycle events atomically. */
  private async persistCompletionOperation(
    completion: RunnerCompletion,
    artifacts: ArtifactMetadata[],
    memory: MemoryMetadata | undefined,
    memoryError: JobError | undefined,
    span: TraceSpan,
    row: JobRow,
  ): Promise<void> {
    if (terminalStatuses.has(row.status)) {
      span.setAttribute("agent_runner.outcome", "noop");
      return;
    }
    const finishedAt = Date.now();
    const status: JobStatus = completion.status;
    const usageJson = completion.usage
      ? JSON.stringify(completion.usage)
      : null;
    const failure = completion.error;
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(
        `UPDATE job SET status = ?, finished_at = ?, current_step = NULL, steps_json = ?, artifacts_json = ?, artifact_error_json = ?, memory_json = ?, memory_error_json = ?, usage_json = ?,
       error_code = ?, error_message = ?, error_retryable = ? WHERE singleton = 1`,
        status,
        finishedAt,
        JSON.stringify(completion.steps),
        JSON.stringify(artifacts),
        completion.artifact_error
          ? JSON.stringify(completion.artifact_error)
          : null,
        memory ? JSON.stringify(memory) : null,
        memoryError ? JSON.stringify(memoryError) : null,
        usageJson,
        failure?.code ?? null,
        failure?.message ?? null,
        failure ? Number(failure.retryable) : null,
      );
      if (historyEnabled) {
        const terminalEvents: FlowEventInput[] = [];
        const memoryEventCount = row.memory_enabled ? 1 : 0;
        const artifactEventCount =
          artifacts.length + (completion.artifact_error ? 1 : 0);
        let terminalSequence =
          Number.MAX_SAFE_INTEGER - memoryEventCount - artifactEventCount;
        if (row.memory_enabled) {
          terminalEvents.push(
            memory
              ? {
                  type: "workflow.memory.persisted",
                  sequence: terminalSequence++,
                  level: "info",
                  data: memory,
                  occurredAt: Number.isFinite(Date.parse(memory.persisted_at))
                    ? Date.parse(memory.persisted_at)
                    : undefined,
                }
              : {
                  type: "workflow.memory.persistence_failed",
                  sequence: terminalSequence++,
                  level: "error",
                  data: {
                    key: row.memory_key,
                    error: memoryError ?? {
                      code: "memory_persist_failed",
                      message: "Memory was not persisted",
                      retryable: true,
                    },
                  },
                },
          );
        }
        for (const [index, artifact] of artifacts.entries()) {
          terminalEvents.push({
            type: "artifact.created",
            sequence: terminalSequence + index,
            level: "info",
            data: artifact,
          });
        }
        terminalSequence += artifacts.length;
        if (completion.artifact_error) {
          terminalEvents.push({
            type: "artifact.collection_failed",
            sequence: terminalSequence,
            level: "error",
            data: { error: completion.artifact_error },
          });
        }
        terminalEvents.push({
          type: "run.finished",
          sequence: Number.MAX_SAFE_INTEGER,
          level: lifecycleLevel(status),
          data: {
            status,
            usage: completion.usage,
            error: completion.error,
            artifact_error: completion.artifact_error ?? null,
            artifact_count: artifacts.length,
            memory: memory ?? null,
            memory_error: memoryError ?? null,
            workflow_version: completion.workflow_digest,
          },
        });
        for (let index = 0; index < terminalEvents.length; index += 100) {
          const terminalChunk = terminalEvents.slice(index, index + 100);
          const closesRun = index + 100 >= terminalEvents.length;
          this.addEventBatch(
            this.eventBatch(
              row.job_id,
              row.created_at,
              status,
              terminalChunk,
              closesRun ? [this.runSpan(row, status, finishedAt)] : [],
            ),
          );
        }
      }
    });
    await this.requestEventOutboxFlush();
    await this.releaseCapacity(row.job_id);
    span.setAttribute("agent_runner.outcome", "success");
  }

  /** Marks an active job cancelled, releases capacity, and stops its container. */
  async cancelJob(): Promise<JobView | null> {
    return traceAsync(tracing, spanNames.jobCancel, {}, (span) =>
      this.cancelJobOperation(span),
    );
  }

  /** Requests cancellation from the active supervisor. */
  private async cancelJobOperation(span: TraceSpan): Promise<JobView | null> {
    const row = this.row();
    setJobAttributes(span, row);
    if (!row) {
      span.setAttribute("agent_runner.outcome", "not_found");
      return null;
    }
    if (terminalStatuses.has(row.status)) {
      span.setAttribute("agent_runner.outcome", "noop");
      return this.view(row);
    }

    if (
      row.status === "waiting_for_input" ||
      row.status === "sleeping" ||
      row.status === "budget_suspended" ||
      row.status === "resuming"
    ) {
      const now = Date.now();
      const pendingInteraction = this.pendingInteraction();
      this.ctx.storage.sql.exec(
        "UPDATE interaction SET status = 'resolved', response_json = ?, resolved_at = ? WHERE status = 'pending'",
        JSON.stringify({ action: "cancel" }),
        now,
      );
      if (pendingInteraction) await this.requestInteractionMessageUpdate();
      await this.finishWithError(
        "cancelled",
        "cancelled",
        "Job was cancelled",
        false,
      );
      await this.releaseCapacity(row.job_id);
      if (row.checkpoint_key)
        await this.env.RUNNER_STORAGE.delete(row.checkpoint_key);
      if (row.workflow)
        await this.env.WORKFLOW_BUDGET.get(
          this.env.WORKFLOW_BUDGET.idFromName(row.workflow),
        ).remove(row.job_id);
      span.setAttribute("agent_runner.outcome", "success");
      return this.view(this.requireRow());
    }

    this.ctx.storage.sql.exec(
      "UPDATE job SET status = 'cancelling', finalization_deadline_at = COALESCE(finalization_deadline_at, ?) WHERE singleton = 1",
      Date.now() + appConfig.memoryLimits.persistenceTimeoutMs,
    );
    let requested = true;
    try {
      await this.containerFetch("http://container.internal/cancel", {
        method: "POST",
      });
    } catch (error) {
      requested = false;
      span.setAttribute("error.type", errorType(error));
      console.error(
        JSON.stringify({
          level: "error",
          source: "worker",
          job_id: row.job_id,
          trace_id: row.trace_id,
          span_id: row.run_span_id,
          event: "cancel_request_failed",
          error: errorMessage(error),
        }),
      );
    }
    this.renewActivityTimeout();
    await this.schedule(1, "collectResult");
    span.setAttribute("agent_runner.outcome", requested ? "success" : "error");
    return this.view(this.requireRow());
  }

  /** Expires retained state and interrupts a job if it somehow remains active. */
  async expireJob(): Promise<void> {
    return traceAsync(tracing, spanNames.jobExpire, {}, (span) =>
      this.expireJobOperation(span),
    );
  }

  /** Removes one job and its retained artifacts after the retention window. */
  private async expireJobOperation(span: TraceSpan): Promise<void> {
    const row = this.row();
    setJobAttributes(span, row);
    if (!row) {
      span.setAttribute("agent_runner.outcome", "noop");
      return;
    }
    if (row.expires_at > Date.now()) {
      await this.schedule(new Date(row.expires_at), "expireJob");
      span.setAttribute("agent_runner.outcome", "deferred");
      return;
    }
    if (!terminalStatuses.has(row.status)) {
      await this.finishWithError(
        "interrupted",
        "retention_expired",
        "Job exceeded its retention window",
        false,
        unpersistedMemoryError(
          row,
          "Container expired before memory could be persisted",
        ),
      );
      try {
        await this.stop("SIGTERM");
      } catch {
        /* already stopped */
      }
    }
    await this.releaseCapacity(row.job_id);
    if (row.workflow)
      await this.env.WORKFLOW_BUDGET.get(
        this.env.WORKFLOW_BUDGET.idFromName(row.workflow),
      ).remove(row.job_id);
    if (this.eventFlush) await this.eventFlush;
    await this.deleteArtifacts(row.job_id);
    if (row.checkpoint_key)
      await this.env.RUNNER_STORAGE.delete(row.checkpoint_key);
    await this.ctx.storage.deleteAll();
    span.setAttribute("agent_runner.outcome", "success");
  }

  /** Converts unexpected container termination into a terminal job outcome. */
  override async onStop(params: {
    exitCode: number;
    reason: "exit" | "runtime_signal";
  }): Promise<void> {
    return traceAsync(
      tracing,
      spanNames.containerStop,
      {
        "agent_runner.container.exit_code": params.exitCode,
        "agent_runner.container.stop_reason": params.reason,
      },
      (span) => this.onStopOperation(params, span),
    );
  }

  /** Converts one traced container stop signal into a stable job outcome. */
  private async onStopOperation(
    params: {
      exitCode: number;
      reason: "exit" | "runtime_signal";
    },
    span: TraceSpan,
  ): Promise<void> {
    const row = this.row();
    setJobAttributes(span, row);
    if (
      row &&
      !terminalStatuses.has(row.status) &&
      row.status !== "waiting_for_input" &&
      row.status !== "sleeping" &&
      row.status !== "budget_suspended" &&
      row.status !== "resuming"
    ) {
      const status = row.status === "cancelling" ? "cancelled" : "interrupted";
      await this.finishWithError(
        status,
        "container_interrupted",
        `Container stopped (${params.reason}, exit ${params.exitCode})`,
        true,
        unpersistedMemoryError(
          row,
          "Container stopped before memory could be persisted",
        ),
      );
      await this.releaseCapacity(row.job_id);
    }
    console.log(
      JSON.stringify({
        level: "info",
        source: "worker",
        job_id: row?.job_id,
        workflow: row?.workflow,
        trace_id: row?.trace_id,
        span_id: row?.run_span_id,
        event: "container_stopped",
        ...params,
      }),
    );
    span.setAttribute("agent_runner.outcome", "success");
  }

  /** Marks an active job timed out when the container activity deadline expires. */
  override async onActivityExpired(): Promise<void> {
    return traceAsync(
      tracing,
      spanNames.containerActivityExpire,
      {},
      async (span) => {
        await this.onActivityExpiredOperation(span);
        span.setAttribute("agent_runner.outcome", "timeout");
      },
    );
  }

  /** Applies the authoritative timeout transition and stops the container. */
  private async onActivityExpiredOperation(span: TraceSpan): Promise<void> {
    const row = this.row();
    setJobAttributes(span, row);
    if (
      row &&
      !terminalStatuses.has(row.status) &&
      row.status !== "waiting_for_input" &&
      row.status !== "sleeping" &&
      row.status !== "budget_suspended" &&
      row.status !== "resuming"
    ) {
      await this.finishWithError(
        "timed_out",
        "container_idle_timeout",
        "Container activity deadline expired",
        false,
        unpersistedMemoryError(
          row,
          "Container activity expired before memory could be persisted",
        ),
      );
      await this.releaseCapacity(row.job_id);
    }
    await this.stop("SIGTERM");
  }

  /** Records infrastructure errors before allowing the container framework to rethrow them. */
  override async onError(error: unknown): Promise<never> {
    return traceAsync(tracing, spanNames.containerError, {}, async (span) => {
      setJobAttributes(span, this.row());
      span.setAttribute("error.type", errorType(error));
      span.setAttribute("agent_runner.outcome", "error");
      return this.onErrorOperation(error);
    });
  }

  /** Persists one infrastructure failure before rethrowing it to the runtime. */
  private async onErrorOperation(error: unknown): Promise<never> {
    const row = this.row();
    if (
      row &&
      !terminalStatuses.has(row.status) &&
      row.status !== "sleeping" &&
      row.status !== "budget_suspended" &&
      row.status !== "resuming"
    ) {
      await this.finishWithError(
        "failed",
        "container_error",
        errorMessage(error),
        true,
        unpersistedMemoryError(
          row,
          "Container failed before memory could be persisted",
        ),
      );
      await this.releaseCapacity(row.job_id);
    }
    throw error;
  }

  async getHistory(kind: HistoryKind, limit: number, cursor: string | null) {
    const row = this.row();
    if (!row || row.expires_at <= Date.now()) return null;
    return historyPage(this.ctx.storage.sql, kind, limit, cursor);
  }

  private row(): JobRow | null {
    return (
      this.ctx.storage.sql
        .exec<JobRow>("SELECT * FROM job WHERE singleton = 1")
        .toArray()[0] ?? null
    );
  }

  private requireRow(): JobRow {
    const row = this.row();
    if (!row) throw new Error("job state is missing");
    return row;
  }

  /** Maps the persisted SQL row into the stable public job representation. */
  private view(row: JobRow): JobView {
    const pendingEventBatches = this.pendingEventBatches();
    const view: JobView = {
      job_id: row.job_id,
      workflow: row.workflow ?? "unknown",
      workflow_version: row.workflow_version ?? "unknown",
      trigger: jobTrigger(row),
      current_step: row.current_step,
      status: row.status,
      created_at: toIso(row.created_at)!,
      started_at: toIso(row.started_at),
      finished_at: toIso(row.finished_at),
      expires_at: toIso(row.expires_at)!,
      history: historyView(
        this.ctx.storage.sql,
        row.event_last_error,
        toIso(row.event_updated_at),
      ),
    };
    if (row.status === "budget_suspended" && row.budget_reset_at !== null)
      view.resume_at = toIso(row.budget_reset_at)!;
    const pending = this.pendingInteraction();
    if (pending && pending.expires_at > Date.now()) {
      const request = JSON.parse(pending.request_json) as ElicitationRequest;
      view.pending_interaction = {
        id: pending.id,
        mode: request.mode,
        message: request.message,
        created_at: new Date(pending.created_at).toISOString(),
        expires_at: new Date(pending.expires_at).toISOString(),
      };
    }
    if (row.steps_json) {
      view.result = {
        steps: JSON.parse(row.steps_json) as StepResult[],
        usage: row.usage_json
          ? (JSON.parse(row.usage_json) as SessionUsage)
          : null,
        artifacts: row.artifacts_json
          ? (JSON.parse(row.artifacts_json) as ArtifactMetadata[])
          : [],
        ...(row.artifact_error_json
          ? {
              artifact_error: JSON.parse(
                row.artifact_error_json,
              ) as JobView["error"],
            }
          : {}),
        ...(row.memory_json
          ? { memory: JSON.parse(row.memory_json) as MemoryMetadata }
          : {}),
        ...(row.memory_error_json
          ? {
              memory_error: JSON.parse(
                row.memory_error_json,
              ) as JobView["error"],
            }
          : {}),
      };
    }
    if (
      terminalStatuses.has(row.status) &&
      row.status !== "succeeded" &&
      row.status !== "partial"
    ) {
      view.error = {
        code: row.error_code ?? row.status,
        message: row.error_message ?? `Job ${row.status}`,
        retryable: Boolean(row.error_retryable),
      };
    }
    return view;
  }

  /** Persists a bounded terminal error for a job that cannot produce a result. */
  private async finishWithError(
    status: Exclude<
      JobStatus,
      | "starting"
      | "running"
      | "waiting_for_input"
      | "sleeping"
      | "budget_suspended"
      | "resuming"
      | "cancelling"
      | "succeeded"
    >,
    code: string,
    message: string,
    retryable: boolean,
    memoryError?: JobError,
  ): Promise<void> {
    const current = this.requireRow();
    if (terminalStatuses.has(current.status)) return;
    const finishedAt = Date.now();
    this.ctx.storage.transactionSync(() => {
      this.ctx.storage.sql.exec(
        "UPDATE job SET status = ?, finished_at = ?, error_code = ?, error_message = ?, error_retryable = ?, memory_error_json = ? WHERE singleton = 1",
        status,
        finishedAt,
        code,
        message.slice(0, 4096),
        Number(retryable),
        memoryError ? JSON.stringify(memoryError) : null,
      );
      if (historyEnabled) {
        const row = this.requireRow();
        const terminalEvents: FlowEventInput[] = [];
        if (memoryError) {
          const materializationFailed =
            memoryError.code === "memory_load_failed";
          terminalEvents.push({
            type: materializationFailed
              ? "workflow.memory.materialization_failed"
              : "workflow.memory.persistence_failed",
            sequence: materializationFailed
              ? memoryMaterializedSequence
              : Number.MAX_SAFE_INTEGER - 1,
            level: "error",
            data: { key: row.memory_key, error: memoryError },
          });
        }
        terminalEvents.push({
          type: "run.finished",
          sequence: Number.MAX_SAFE_INTEGER,
          level: lifecycleLevel(status),
          data: {
            status,
            error: { code, message, retryable },
            artifact_error: null,
            artifact_count: 0,
            memory: null,
            memory_error: memoryError ?? null,
            workflow_version: row.workflow_version,
          },
        });
        this.addEventBatch(
          this.eventBatch(row.job_id, row.created_at, status, terminalEvents, [
            this.runSpan(row, status, finishedAt),
          ]),
        );
      }
    });
    await this.requestEventOutboxFlush();
  }

  private async releaseCapacity(jobId: string): Promise<void> {
    const coordinator = this.env.JOB_COORDINATOR.get(
      this.env.JOB_COORDINATOR.idFromName("global"),
    );
    await coordinator.release(jobId);
  }

  private async deleteArtifacts(jobId: string): Promise<void> {
    const prefix = `jobs/${jobId}/`;
    let cursor: string | undefined;
    do {
      const listed = await this.env.RUNNER_STORAGE.list({ prefix, cursor });
      if (listed.objects.length > 0)
        await this.env.RUNNER_STORAGE.delete(
          listed.objects.map((object) => object.key),
        );
      cursor = listed.truncated ? listed.cursor : undefined;
    } while (cursor);
  }

  private addEventBatch(payload: string, reservedLifecycle = true): void {
    this.ctx.storage.transactionSync(() => {
      const filtered = appendHistory(
        this.ctx.storage.sql,
        payload,
        reservedLifecycle
          ? {
              ...appConfig.historyLimits,
              maxPendingBytes:
                appConfig.historyLimits.maxPendingBytes +
                appConfig.historyLimits.maxLifecycleBytes,
            }
          : appConfig.historyLimits,
      );
      if (!filtered) return;
      payload = filtered;
      this.ctx.storage.sql.exec(
        "INSERT OR IGNORE INTO event_outbox(id, payload, created_at) VALUES (?, ?, ?)",
        crypto.randomUUID(),
        payload,
        Date.now(),
      );
      this.ctx.storage.sql.exec(
        "UPDATE job SET event_received_batches = event_received_batches + 1, event_updated_at = ? WHERE singleton = 1",
        Date.now(),
      );
    });
  }

  /** Schedules a durable retry and begins delivery without delaying the callback response. */
  private async requestEventOutboxFlush(): Promise<void> {
    await this.schedule(1, "flushEventOutbox");
    this.ctx.waitUntil(this.flushEventOutbox());
  }

  private pendingEventBatches(): number {
    return this.ctx.storage.sql
      .exec<{ count: number }>("SELECT COUNT(*) AS count FROM event_outbox")
      .one().count;
  }

  private recordDeliveryError(message: string): void {
    this.ctx.storage.sql.exec(
      "UPDATE job SET event_last_error = ?, event_updated_at = ? WHERE singleton = 1",
      message.slice(0, 1024),
      Date.now(),
    );
  }

  private lifecycleBatch(
    jobId: string,
    createdAt: number,
    status: JobStatus,
    type: string,
    sequence: number,
    data: object,
    closesRun = false,
  ): string {
    const row = this.requireRow();
    return this.eventBatch(
      jobId,
      createdAt,
      status,
      [{ type, sequence, level: lifecycleLevel(status), data }],
      closesRun && row.finished_at !== null
        ? [this.runSpan(row, status, row.finished_at)]
        : [],
    );
  }

  /** Builds the completed portable root span for a terminal workflow run. */
  private runSpan(
    row: JobRow,
    status: JobStatus,
    finishedAt: number,
  ): FlowSpanRecord {
    return {
      trace_id: row.trace_id,
      span_id: row.run_span_id,
      parent_span_id: null,
      name: "workflow.run",
      kind: "INTERNAL",
      started_at: new Date(row.created_at).toISOString(),
      finished_at: new Date(finishedAt).toISOString(),
      status: { code: lifecycleSpanStatus(status) },
      attributes: {
        "agent_runner.job.id": row.job_id,
        "agent_runner.workflow.name": row.workflow ?? "unknown",
        "agent_runner.workflow.version": row.workflow_version ?? "unknown",
        "agent_runner.job.status": status,
        "agent_runner.trigger.type": row.trigger_type,
        ...(row.trigger_schedule_id
          ? { "agent_runner.schedule.id": row.trigger_schedule_id }
          : {}),
        ...(row.trigger_cron
          ? { "agent_runner.schedule.cron": row.trigger_cron }
          : {}),
        ...(row.trigger_scheduled_at !== null
          ? { "agent_runner.schedule.scheduled_at": row.trigger_scheduled_at }
          : {}),
      },
    };
  }

  /** Encodes one protocol-v4 Flow batch with correlated events, spans, and metric points. */
  private eventBatch(
    jobId: string,
    createdAt: number,
    status: JobStatus,
    events: FlowEventInput[],
    spans: FlowSpanRecord[] = [],
  ): string {
    const row = this.requireRow();
    const finishedAt = terminalStatuses.has(status)
      ? toIso(row?.finished_at ?? null)
      : null;
    const occurredAt = finishedAt ?? new Date().toISOString();
    return JSON.stringify({
      schema_version: 4,
      run: {
        source_run_id: jobId,
        harness: flowRunHarness,
        status,
        created_at: new Date(createdAt).toISOString(),
        started_at:
          status === "starting" ? null : toIso(row?.started_at ?? null),
        finished_at: finishedAt,
        trace_id: row.trace_id,
        root_span_id: row.run_span_id,
        default_harness: row.workflow_default_harness,
        default_model: row.workflow_default_model,
        default_reasoning_effort: row.workflow_default_reasoning_effort,
      },
      events: events.map((event) => ({
        event_id: `${jobId}:${event.sequence}:0`,
        sequence: event.sequence,
        chunk_index: 0,
        step_id: null,
        step_index: null,
        step_kind: null,
        type: event.type,
        source: "runner",
        level: event.level,
        protocol_version: 4,
        trace_id: row.trace_id,
        span_id: row.run_span_id,
        data: event.type.startsWith("run.")
          ? { ...event.data, trigger: jobTrigger(row) }
          : event.data,
        occurred_at:
          event.occurredAt !== undefined
            ? new Date(event.occurredAt).toISOString()
            : event.type === "run.created"
              ? new Date(createdAt).toISOString()
              : event.type === "run.started"
                ? (toIso(row?.started_at ?? null) ?? occurredAt)
                : occurredAt,
      })),
      spans,
      metrics: [],
    });
  }
}

// Assignment is intentional: it invokes @cloudflare/containers' inherited
// registration setter. A native static class field would shadow that setter.
AgentContainer.outbound = injectCredential;
AgentContainer.outboundHandlers = { credentialProxy: injectCredential };
