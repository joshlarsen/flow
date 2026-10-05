vi.mock("../src/generated-config.ts",async(importOriginal)=>{const original=await importOriginal<typeof import("../src/generated-config.ts")>();return {...original,appConfig:{...original.appConfig,interactions:{provider:"slack",teamId:"T123",conversationId:"C456",allowedUserIds:["U789"],botTokenSecret:"SLACK_BOT_TOKEN",signingSecret:"SLACK_SIGNING_SECRET",maxRequestBytes:65536,maxResponseBytes:65536,responseTtlMs:86400000,liveWaitMs:30000,checkpointLimits:original.appConfig.checkpointLimits}}};});
import { afterEach, describe, expect, it, vi } from "vitest";
import { appConfig } from "../src/generated-config.ts";
import { parseInteractionDelivery } from "../src/interaction-provider.ts";
import { interactionContinuesCurrentPrompt, parseElicitationRequest, validateElicitationContent } from "../src/interactions.ts";
import {
  completedSlackElicitation,
  handleSlackInteraction,
  postSlackElicitation,
  SlackAPIError,
  updateSlackElicitation,
  verifySlackRequest,
} from "../src/slack-interactions.ts";
import type { Env } from "../src/types.ts";

async function signature(secret: string, timestamp: string, body: string): Promise<string> {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const bytes = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(`v0:${timestamp}:${body}`)));
  return `v0=${[...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

const interactionReference = {
  job_id: "123e4567-e89b-42d3-a456-426614174000",
  interaction_id: "123e4567-e89b-42d3-a456-426614174001",
};
const interactionOrigin = { workflow: "deploy", stepId: "choose-target" };
const resolvedAt = Date.parse("2026-09-16T14:30:00.000Z");

function configuredSlackForTest() {
  if (appConfig.interactions.provider !== "slack") throw new Error("test requires Slack interactions");
  return appConfig.interactions;
}

function slackEnv(stub: object): Env {
  const config = configuredSlackForTest();
  return {
    [config.botTokenSecret]: "xoxb-test",
    [config.signingSecret]: "signing-secret",
    AGENT_CONTAINER: {
      idFromName: (name: string) => name,
      get: () => stub,
    },
  } as unknown as Env;
}

async function signedSlackRequest(payload: object): Promise<Request> {
  const timestamp = String(Math.floor(Date.now() / 1000));
  const body = new URLSearchParams({ payload: JSON.stringify(payload) }).toString();
  return new Request("https://runner.test/v1/interactions/slack", {
    method: "POST",
    headers: {
      "content-type": "application/x-www-form-urlencoded",
      "x-slack-request-timestamp": timestamp,
      "x-slack-signature": await signature("signing-secret", timestamp, body),
    },
    body,
  });
}

function slackBlockAction(actionId = "elicitation_open") {
  const config = configuredSlackForTest();
  return {
    type: "block_actions",
    team: { id: config.teamId },
    user: { id: config.allowedUserIds[0] },
    channel: { id: config.conversationId },
    trigger_id: "trigger-1",
    actions: [{ action_id: actionId, value: JSON.stringify(interactionReference) }],
  };
}

function slackModalCallback(type: "view_submission" | "view_closed") {
  const config = configuredSlackForTest();
  return {
    type,
    team: { id: config.teamId },
    user: { id: config.allowedUserIds[0] },
    view: {
      callback_id: "agent_runner_elicitation",
      private_metadata: JSON.stringify(interactionReference),
      state: { values: {} },
    },
  };
}

describe("ACP elicitation forms", () => {
  it("continues the active prompt only for accepted form content", () => {
    expect(interactionContinuesCurrentPrompt({ action: "accept", content: { answer: "yes" } })).toBe(true);
    expect(interactionContinuesCurrentPrompt({ action: "decline" })).toBe(false);
    expect(interactionContinuesCurrentPrompt({ action: "cancel" })).toBe(false);
  });

  it("accepts supported primitive fields and validates submitted values", () => {
    const request = parseElicitationRequest({
      mode: "form",
      message: "Choose deployment settings",
      requestedSchema: {
        type: "object",
        properties: {
          environment: { type: "string", enum: ["staging", "production"] },
          replicas: { type: "integer", minimum: 1, maximum: 10 },
          notify: { type: "boolean" },
          regions: { type: "array", items: { type: "string", enum: ["iad", "lhr"] }, minItems: 1 },
        },
        required: ["environment", "replicas"],
      },
    });
    expect(validateElicitationContent(request.requestedSchema, { environment: "staging", replicas: 2, notify: true, regions: ["iad"] })).toBeNull();
    expect(validateElicitationContent(request.requestedSchema, { environment: "other", replicas: 2 })).toMatch(/invalid choice/);
    expect(validateElicitationContent(request.requestedSchema, { environment: "staging", replicas: 1.5 })).toMatch(/integer/);
  });

  it("rejects URL mode and non-primitive schemas", () => {
    expect(() => parseElicitationRequest({ mode: "url", message: "Open", url: "https://example.com" })).toThrow();
    expect(() => parseElicitationRequest({ mode: "form", message: "Nested", requestedSchema: { type: "object", properties: { nested: { type: "object" } } } })).toThrow();
  });

  it("rejects forms with more than five questions", () => {
    const properties = Object.fromEntries(Array.from({ length: 6 }, (_, index) => [`field_${index}`, { type: "string" }]));
    expect(() => parseElicitationRequest({ mode: "form", message: "Too many", requestedSchema: { type: "object", properties } })).toThrow(/1 to 5/);
  });

  it("counts recognized native custom-answer fields as companions", () => {
    const properties = Object.fromEntries(Array.from({ length: 5 }, (_, index) => [
      [`question_${index}`, { type: "string", oneOf: [
        { const: "yes", title: "Yes", description: "Continue" },
        { const: "no", title: "No", description: "Stop" },
      ] }],
      [`question_${index}_custom`, {
        type: "string",
        title: "Other",
        _meta: { _askUserQuestionCustomAnswer: { questionId: `question_${index}`, isCustomAnswer: true } },
      }],
    ]).flat());
    const request = parseElicitationRequest({ mode: "form", message: "Answer", requestedSchema: { type: "object", properties } });
    expect(Object.keys(request.requestedSchema.properties)).toHaveLength(10);
    expect(request.requestedSchema.properties.question_0?.oneOf?.[0]?.description).toBe("Continue");
  });

  it("rejects malformed custom-answer companions", () => {
    expect(() => parseElicitationRequest({
      mode: "form",
      message: "Answer",
      requestedSchema: { type: "object", properties: {
        answer: { type: "string", oneOf: [{ const: "yes" }] },
        custom: { type: "string", _meta: { _askUserQuestionCustomAnswer: { questionId: "missing", isCustomAnswer: true } } },
      } },
    })).toThrow(/custom-answer metadata/);
  });
});

describe("Slack request verification", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("accepts the exact signed body and rejects tampering or stale timestamps", async () => {
    const now = Date.now();
    const timestamp = String(Math.floor(now / 1000));
    const body = "payload=%7B%22type%22%3A%22block_actions%22%7D";
    const request = new Request("https://runner.test/v1/interactions/slack", { method: "POST", headers: {
      "x-slack-request-timestamp": timestamp,
      "x-slack-signature": await signature("secret", timestamp, body),
    }, body });
    expect(await verifySlackRequest(request, body, "secret", now)).toBe(true);
    expect(await verifySlackRequest(request, `${body}x`, "secret", now)).toBe(false);
    expect(await verifySlackRequest(request, body, "secret", now + 301_000)).toBe(false);
  });

  it("captures and validates the posted Slack message reference", async () => {
    if (appConfig.interactions.provider !== "slack") throw new Error("test requires Slack interactions");
    const request = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({
      ok: true,
      channel: appConfig.interactions.conversationId,
      ts: "1723456789.123456",
    }), { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", request);
    const env = { [appConfig.interactions.botTokenSecret]: "xoxb-test" } as unknown as Env;

    const delivery = await postSlackElicitation(env, {
      job_id: "123e4567-e89b-42d3-a456-426614174000",
      interaction_id: "123e4567-e89b-42d3-a456-426614174001",
    }, "Choose a deployment target", interactionOrigin);

    expect(delivery).toEqual({ channel: appConfig.interactions.conversationId, messageTs: "1723456789.123456" });
    expect(parseInteractionDelivery({ provider: "slack", reference: delivery, origin: interactionOrigin })).toEqual({ provider: "slack", reference: delivery, origin: interactionOrigin });
    expect(parseInteractionDelivery({ provider: "slack", reference: delivery })).toEqual({ provider: "slack", reference: delivery });
    expect(parseInteractionDelivery({ provider: "slack", reference: delivery, origin: { workflow: "bad workflow", stepId: "choose-target" } })).toEqual({ provider: "slack", reference: delivery });
    expect(parseInteractionDelivery({ provider: "slack", reference: { channel: "bad channel", messageTs: "nope" } })).toBeNull();
    expect(parseInteractionDelivery({ provider: "slack", reference: { channel: "COTHER", messageTs: "1723456789.123456" } })).toBeNull();
    expect(request).toHaveBeenCalledOnce();
    expect(request.mock.calls[0]?.[0]).toBe("https://slack.com/api/chat.postMessage");
    const payload = JSON.parse(String((request.mock.calls[0]?.[1] as RequestInit).body));
    expect(payload.blocks[1].type).toBe("actions");
    expect(payload.blocks[2]).toMatchObject({
      type: "context",
      elements: [{ type: "mrkdwn", text: "Requested by *deploy* · step *choose-target*" }],
    });
  });

  it("opens a pending interaction after the live-response window", async () => {
    const getInteractionForProvider = vi.fn(async () => ({
      state: "pending" as const,
      request: parseElicitationRequest({
        mode: "form",
        message: "Continue?",
        requestedSchema: { type: "object", properties: { answer: { type: "string" } } },
      }),
    }));
    const stub = { getInteractionForProvider, resolveInteractionFromProvider: vi.fn() };
    const slackAPI = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", slackAPI);

    const response = await handleSlackInteraction(await signedSlackRequest(slackBlockAction()), slackEnv(stub));

    expect(response.status).toBe(200);
    expect(getInteractionForProvider).toHaveBeenCalledWith(interactionReference.interaction_id, configuredSlackForTest().allowedUserIds[0]);
    expect(slackAPI).toHaveBeenCalledOnce();
    expect(slackAPI.mock.calls[0]?.[0]).toBe("https://slack.com/api/views.open");
    const body = JSON.parse(String((slackAPI.mock.calls[0]?.[1] as RequestInit).body));
    expect(body.view.callback_id).toBe("agent_runner_elicitation");
  });

  it.each([
    ["resolved", "already been answered"],
    ["expired", "has expired"],
    ["missing", "no longer available"],
  ] as const)("acknowledges a %s Respond action and opens informational feedback", async (state, message) => {
    const stub = {
      getInteractionForProvider: vi.fn(async () => ({ state })),
      resolveInteractionFromProvider: vi.fn(),
    };
    const slackAPI = vi.fn<typeof fetch>(async () => new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", slackAPI);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const response = await handleSlackInteraction(await signedSlackRequest(slackBlockAction()), slackEnv(stub));

    expect(response.status).toBe(200);
    expect(slackAPI).toHaveBeenCalledOnce();
    const body = JSON.parse(String((slackAPI.mock.calls[0]?.[1] as RequestInit).body));
    expect(JSON.stringify(body.view)).toContain(message);
    expect(warning).toHaveBeenCalledOnce();
    const diagnostic = String(warning.mock.calls[0]?.[0]);
    expect(diagnostic).toContain(`"interaction_state":"${state}"`);
    expect(diagnostic).not.toContain("requestedSchema");
    expect(diagnostic).not.toContain("content");
  });

  it("idempotently acknowledges a duplicate decline", async () => {
    const resolveInteractionFromProvider = vi.fn(async () => ({ accepted: false, state: "resolved" as const }));
    const stub = { getInteractionForProvider: vi.fn(), resolveInteractionFromProvider };
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const response = await handleSlackInteraction(
      await signedSlackRequest(slackBlockAction("elicitation_decline")),
      slackEnv(stub),
    );

    expect(response.status).toBe(200);
    expect(resolveInteractionFromProvider).toHaveBeenCalledWith(
      interactionReference.interaction_id,
      configuredSlackForTest().allowedUserIds[0],
      { action: "decline" },
    );
    expect(warning).toHaveBeenCalledOnce();
  });

  it.each(["view_submission", "view_closed"] as const)("idempotently acknowledges a duplicate %s callback", async (type) => {
    const stub = {
      getInteractionForProvider: vi.fn(async () => ({ state: "resolved" as const })),
      resolveInteractionFromProvider: vi.fn(),
    };
    const slackAPI = vi.fn<typeof fetch>();
    vi.stubGlobal("fetch", slackAPI);
    const warning = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const response = await handleSlackInteraction(await signedSlackRequest(slackModalCallback(type)), slackEnv(stub));

    expect(response.status).toBe(200);
    expect(stub.resolveInteractionFromProvider).not.toHaveBeenCalled();
    expect(slackAPI).not.toHaveBeenCalled();
    expect(warning).toHaveBeenCalledOnce();
  });

  it("acknowledges an unavailable Respond action when Slack feedback fails", async () => {
    const stub = {
      getInteractionForProvider: vi.fn(async () => ({ state: "missing" as const })),
      resolveInteractionFromProvider: vi.fn(),
    };
    vi.stubGlobal("fetch", vi.fn<typeof fetch>(async () => { throw new Error("Slack unavailable"); }));
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    const response = await handleSlackInteraction(await signedSlackRequest(slackBlockAction()), slackEnv(stub));

    expect(response.status).toBe(200);
  });

  it("renders every terminal state without controls or submitted content", () => {
    const outcomes = [
      [{ status: "answered", actorId: "U123", resolvedAt } as const, "Answered by <@U123>"],
      [{ status: "declined", actorId: "U234", resolvedAt } as const, "Declined by <@U234>"],
      [{ status: "cancelled", actorId: "U345", resolvedAt } as const, "Cancelled by <@U345>"],
      [{ status: "expired" } as const, "Expired without a response"],
      [{ status: "run_cancelled" } as const, "Cancelled because the agent run was cancelled"],
    ] as const;
    for (const [outcome, status] of outcomes) {
      const rendered = completedSlackElicitation("Choose a deployment target", outcome, interactionOrigin);
      expect(JSON.stringify(rendered)).toContain(status);
      expect(JSON.stringify(rendered)).toContain("Requested by *deploy* · step *choose-target*");
      if ("resolvedAt" in outcome) expect(JSON.stringify(rendered)).toContain("<!date^1789569000^{date_short_pretty} at {time}|2026-09-16T14:30:00.000Z>");
      expect(JSON.stringify(rendered)).not.toContain('"type":"actions"');
      expect(JSON.stringify(rendered)).not.toContain("submitted-secret");
      expect((rendered.blocks[0] as any).text.text).toBe("Choose a deployment target");
    }
  });

  it("updates the original Slack message and preserves rate-limit retry guidance", async () => {
    if (appConfig.interactions.provider !== "slack") throw new Error("test requires Slack interactions");
    const responses = [
      new Response(JSON.stringify({ ok: true }), { headers: { "content-type": "application/json" } }),
      new Response(JSON.stringify({ ok: false, error: "ratelimited" }), { status: 429, headers: { "content-type": "application/json", "retry-after": "17" } }),
    ];
    const request = vi.fn<typeof fetch>(async () => responses.shift()!);
    vi.stubGlobal("fetch", request);
    const env = { [appConfig.interactions.botTokenSecret]: "xoxb-test" } as unknown as Env;
    const delivery = { channel: appConfig.interactions.conversationId, messageTs: "1723456789.123456" };

    await updateSlackElicitation(env, delivery, "Choose a deployment target", { status: "answered", actorId: "U123", resolvedAt }, interactionOrigin);
    expect(request.mock.calls[0]?.[0]).toBe("https://slack.com/api/chat.update");
    const payload = JSON.parse(String((request.mock.calls[0]?.[1] as RequestInit).body));
    expect(payload).toMatchObject({ channel: delivery.channel, ts: delivery.messageTs, text: "Agent input request answered at 2026-09-16T14:30:00.000Z." });
    expect(JSON.stringify(payload)).toContain("Answered by <@U123>");
    expect(JSON.stringify(payload)).toContain("Requested by *deploy* · step *choose-target*");
    expect(JSON.stringify(payload)).not.toContain('"type":"actions"');

    await expect(updateSlackElicitation(env, delivery, "Choose a deployment target", { status: "expired" }))
      .rejects.toMatchObject({ retryAfterSeconds: 17 } satisfies Partial<SlackAPIError>);
  });
});
