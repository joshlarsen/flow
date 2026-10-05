export type TraceAttribute = string | number | boolean | undefined;

export interface FlowTraceContext {
  readonly traceId: string;
  readonly rootSpanId: string;
}

export interface TraceSpan {
  readonly isTraced: boolean;
  setAttribute(key: string, value: string | number | boolean): unknown;
}

export interface Tracer {
  enterSpan<T, A extends unknown[]>(
    name: string,
    callback: (span: TraceSpan, ...args: A) => T,
    ...args: A
  ): T;
}

export const spanNames = {
  request: "agent_runner.request",
  preflight: "agent_runner.preflight",
  jobCreate: "agent_runner.job.create",
  jobGet: "agent_runner.job.get",
  jobStart: "agent_runner.job.start",
  jobCancel: "agent_runner.job.cancel",
  jobComplete: "agent_runner.job.complete",
  jobExpire: "agent_runner.job.expire",
  jobFinalize: "agent_runner.job.finalize",
  eventsReceive: "agent_runner.events.receive",
  capacityAcquire: "agent_runner.capacity.acquire",
  capacityRelease: "agent_runner.capacity.release",
  capacityCleanup: "agent_runner.capacity.cleanup",
  scheduleEnqueue: "agent_runner.schedule.enqueue",
  scheduleDispatch: "agent_runner.schedule.dispatch",
  workflowBundleLoad: "agent_runner.workflow.bundle.load",
  containerBoot: "agent_runner.container.boot",
  workflowStage: "agent_runner.workflow.stage",
  memoryLoad: "agent_runner.memory.load",
  memoryPersist: "agent_runner.memory.persist",
  workflowDispatch: "agent_runner.workflow.dispatch",
  resultCollect: "agent_runner.result.collect",
  artifactsUpload: "agent_runner.artifacts.upload",
  flowEnqueue: "agent_runner.flow.enqueue",
  flowDeliver: "agent_runner.flow.deliver",
  egressForward: "agent_runner.egress.forward",
  oauthToken: "agent_runner.oauth.token",
  containerStop: "agent_runner.container.stop",
  containerActivityExpire: "agent_runner.container.activity_expire",
  containerError: "agent_runner.container.error",
} as const;

const noopSpan: TraceSpan = {
  isTraced: false,
  setAttribute: () => noopSpan,
};

export const noOpTracer: Tracer = {
  enterSpan: (_name, callback, ...args) => callback(noopSpan, ...args),
};

function randomHex(byteLength: number): string {
  const bytes = crypto.getRandomValues(new Uint8Array(byteLength));
  if (bytes.every((value) => value === 0)) bytes[byteLength - 1] = 1;
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
}

/** Creates a portable, nonzero W3C trace identifier and its root span identifier. */
export function newFlowTraceContext(): FlowTraceContext {
  return { traceId: randomHex(16), rootSpanId: randomHex(8) };
}

/** Marks a native Cloudflare span as belonging to a portable Flow run trace. */
export function setFlowTraceAttributes(
  span: TraceSpan,
  jobId?: string,
  traceId?: string,
): void {
  setSpanAttributes(span, {
    "flow.trace": true,
    "flow.trace.id": traceId,
    "flow.run.id": jobId,
  });
}

/** Sets defined scalar attributes without relying on the runtime's newer bulk API. */
export function setSpanAttributes(
  span: TraceSpan,
  attributes: Readonly<Record<string, TraceAttribute>>,
): void {
  for (const [key, value] of Object.entries(attributes)) {
    if (value !== undefined) span.setAttribute(key, value);
  }
}

/** Returns a bounded error classification without copying an error message into telemetry. */
export function errorType(error: unknown): string {
  if (error instanceof Error && error.name) return error.name.slice(0, 128);
  return typeof error;
}

/** Runs one asynchronous operation in a custom span and records thrown failures safely. */
export function traceAsync<T>(
  tracer: Tracer,
  name: string,
  attributes: Readonly<Record<string, TraceAttribute>>,
  callback: (span: TraceSpan) => Promise<T>,
): Promise<T> {
  return tracer.enterSpan(name, async (span) => {
    setSpanAttributes(span, attributes);
    try {
      return await callback(span);
    } catch (error) {
      span.setAttribute("agent_runner.outcome", "error");
      span.setAttribute("error.type", errorType(error));
      throw error;
    }
  });
}

/** Records the HTTP result using bounded, query-friendly outcome values. */
export function recordHttpResult(span: TraceSpan, response: Response): void {
  span.setAttribute("http.response.status_code", response.status);
  span.setAttribute(
    "agent_runner.outcome",
    response.status < 400 ? "success" : response.status < 500 ? "rejected" : "error",
  );
}
