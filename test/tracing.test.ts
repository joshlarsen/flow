import { describe, expect, it } from "vitest";
import {
  noOpTracer,
  newFlowTraceContext,
  recordHttpResult,
  setFlowTraceAttributes,
  setSpanAttributes,
  traceAsync,
  type TraceSpan,
  type Tracer,
} from "../src/tracing.ts";

class CapturedSpan implements TraceSpan {
  readonly isTraced = true;
  readonly attributes: Record<string, string | number | boolean> = {};

  setAttribute(key: string, value: string | number | boolean): this {
    this.attributes[key] = value;
    return this;
  }
}

function capturingTracer() {
  const calls: Array<{ name: string; span: CapturedSpan }> = [];
  const tracer: Tracer = {
    enterSpan: (_name, callback, ...args) => {
      const span = new CapturedSpan();
      calls.push({ name: _name, span });
      return callback(span, ...args);
    },
  };
  return { tracer, calls };
}

describe("Worker tracing helpers", () => {
  it("records defined scalar attributes and HTTP outcomes", async () => {
    const { tracer, calls } = capturingTracer();
    await traceAsync(tracer, "operation", { kept: "value", omitted: undefined }, async (span) => {
      recordHttpResult(span, new Response(null, { status: 409 }));
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]!.name).toBe("operation");
    expect(calls[0]!.span.attributes).toEqual({
      kept: "value",
      "http.response.status_code": 409,
      "agent_runner.outcome": "rejected",
    });
  });

  it("classifies thrown failures without recording their messages", async () => {
    const { tracer, calls } = capturingTracer();
    await expect(traceAsync(tracer, "operation", {}, async () => {
      throw new TypeError("sensitive details");
    })).rejects.toThrow("sensitive details");

    expect(calls[0]!.span.attributes).toEqual({
      "agent_runner.outcome": "error",
      "error.type": "TypeError",
    });
    expect(JSON.stringify(calls[0]!.span.attributes)).not.toContain("sensitive details");
  });

  it("provides a no-op tracer for direct unit invocation", async () => {
    let called = false;
    await traceAsync(noOpTracer, "operation", {}, async (span) => {
      called = true;
      expect(span.isTraced).toBe(false);
      setSpanAttributes(span, { ignored: true });
    });
    expect(called).toBe(true);
  });

  it("creates valid portable IDs and applies Cloudflare search markers", () => {
    const first = newFlowTraceContext();
    const second = newFlowTraceContext();
    expect(first.traceId).toMatch(/^[0-9a-f]{32}$/);
    expect(first.rootSpanId).toMatch(/^[0-9a-f]{16}$/);
    expect(first).not.toEqual(second);

    const span = new CapturedSpan();
    setFlowTraceAttributes(span, "job-1", first.traceId);
    expect(span.attributes).toEqual({
      "flow.trace": true,
      "flow.trace.id": first.traceId,
      "flow.run.id": "job-1",
    });
  });
});
