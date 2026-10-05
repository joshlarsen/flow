import { appConfig } from "./generated-config.ts";
import { apiError, json } from "./http.ts";
import type { Env } from "./types.ts";
import {
  validateElicitationContent,
  type ElicitationProperty,
  type InteractionOrigin,
  type ElicitationRequest,
  type InteractionOutcome,
  type InteractionProviderLookup,
  type InteractionProviderResolution,
  type InteractionProviderState,
  type InteractionResponse,
} from "./interactions.ts";

interface InteractionReference { job_id: string; interaction_id: string }
export interface SlackInteractionDelivery { channel: string; messageTs: string }
interface SlackOption { text: { type: "plain_text"; text: string }; value: string; description?: { type: "plain_text"; text: string } }
interface SlackInteractionStub {
  getInteractionForProvider(interactionId: string, userId: string): Promise<InteractionProviderLookup>;
  resolveInteractionFromProvider(interactionId: string, userId: string, response: InteractionResponse): Promise<InteractionProviderResolution>;
}

const encoder = new TextEncoder();
const slackAck = (): Response => new Response(null, { status: 200, headers: { "cache-control": "no-store" } });

export class SlackAPIError extends Error {
  constructor(message: string, readonly retryAfterSeconds: number | null) {
    super(message);
    this.name = "SlackAPIError";
  }
}

function configuredSlack() {
  return appConfig.interactions.provider === "slack" ? appConfig.interactions : null;
}

function secret(env: Env, name: string): string | null {
  const value = (env as unknown as Record<string, unknown>)[name];
  return typeof value === "string" && value.length > 0 ? value : null;
}

function safeEqual(left: string, right: string): boolean {
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  if (a.length !== b.length) return false;
  let difference = 0;
  for (let index = 0; index < a.length; index++) difference |= a[index]! ^ b[index]!;
  return difference === 0;
}

async function hmacHex(key: string, value: string): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey("raw", encoder.encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const bytes = new Uint8Array(await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(value)));
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Verifies Slack's timestamped signature over the untouched request body. */
export async function verifySlackRequest(request: Request, body: string, signingSecret: string, now = Date.now()): Promise<boolean> {
  const timestamp = request.headers.get("x-slack-request-timestamp");
  const signature = request.headers.get("x-slack-signature");
  if (!timestamp || !signature || !/^v0=[0-9a-f]{64}$/.test(signature)) return false;
  const seconds = Number(timestamp);
  if (!Number.isSafeInteger(seconds) || Math.abs(Math.floor(now / 1000) - seconds) > 300) return false;
  return safeEqual(signature, `v0=${await hmacHex(signingSecret, `v0:${timestamp}:${body}`)}`);
}

function choicePairs(property: ElicitationProperty): Array<{ value: string; title: string; description?: string }> {
  if (property.oneOf) return property.oneOf.map((choice) => ({ value: choice.const, title: choice.title ?? choice.const, description: choice.description }));
  if (property.enum) return property.enum.map((value) => ({ value, title: value }));
  if (property.items?.anyOf) return property.items.anyOf.map((choice) => ({ value: choice.const, title: choice.title ?? choice.const, description: choice.description }));
  return (property.items?.enum ?? []).map((value) => ({ value, title: value }));
}

function slackOptions(property: ElicitationProperty): SlackOption[] {
  return choicePairs(property).map((choice) => ({
    text: { type: "plain_text", text: choice.title.slice(0, 75) },
    value: choice.value.slice(0, 75),
    ...(choice.description ? { description: { type: "plain_text" as const, text: choice.description.slice(0, 75) } } : {}),
  }));
}

function fieldElement(name: string, property: ElicitationProperty): Record<string, unknown> {
  const action_id = `field:${name}`;
  const options = slackOptions(property);
  if (property.type === "array") {
    const defaults = Array.isArray(property.default) ? new Set(property.default) : new Set<string>();
    return { type: "multi_static_select", action_id, options, initial_options: options.filter((item) => defaults.has(item.value)) };
  }
  if (options.length > 0) {
    const initial = options.find((item) => item.value === property.default);
    return { type: "static_select", action_id, options, ...(initial ? { initial_option: initial } : {}) };
  }
  if (property.type === "boolean") {
    const option: SlackOption = { text: { type: "plain_text", text: "Yes" }, value: "true" };
    return { type: "checkboxes", action_id, options: [option], ...(property.default === true ? { initial_options: [option] } : {}) };
  }
  return {
    type: "plain_text_input",
    action_id,
    multiline: property.type === "string" && (property.maxLength ?? 0) > 150,
    ...(property.default !== undefined ? { initial_value: String(property.default) } : {}),
    ...(property.minLength !== undefined ? { min_length: property.minLength } : {}),
    ...(property.maxLength !== undefined ? { max_length: Math.min(property.maxLength, 3000) } : {}),
  };
}

function modal(reference: InteractionReference, request: ElicitationRequest): Record<string, unknown> {
  const required = new Set(request.requestedSchema.required ?? []);
  return {
    type: "modal",
    callback_id: "agent_runner_elicitation",
    private_metadata: JSON.stringify(reference),
    notify_on_close: true,
    title: { type: "plain_text", text: (request.requestedSchema.title ?? "Agent input").slice(0, 24) },
    submit: { type: "plain_text", text: "Submit" },
    close: { type: "plain_text", text: "Cancel" },
    blocks: Object.entries(request.requestedSchema.properties).map(([name, property]) => ({
      type: "input",
      block_id: `block:${name}`,
      optional: !required.has(name),
      label: { type: "plain_text", text: (property.title ?? name).slice(0, 200) },
      ...(property.description ? { hint: { type: "plain_text", text: property.description.slice(0, 2000) } } : {}),
      element: fieldElement(name, property),
    })),
  };
}

function unavailableModal(state: "resolved" | "expired" | "missing"): Record<string, unknown> {
  const message = state === "resolved"
    ? "This request has already been answered."
    : state === "expired"
      ? "This request has expired."
      : "This request is no longer available.";
  return {
    type: "modal",
    title: { type: "plain_text", text: "Request unavailable" },
    close: { type: "plain_text", text: "Close" },
    blocks: [{ type: "section", text: { type: "plain_text", text: message } }],
  };
}

function originFootnote(origin: InteractionOrigin): Record<string, unknown> {
  return {
    type: "context",
    elements: [{ type: "mrkdwn", text: `Requested by *${origin.workflow}* · step *${origin.stepId}*` }],
  };
}

/** Records an ignored provider callback without logging its schema or submitted content. */
function logIgnoredInteraction(reference: InteractionReference, payload: Record<string, any>, state: InteractionProviderState): void {
  console.warn(JSON.stringify({
    level: "warn",
    source: "worker",
    event: "slack_interaction_ignored",
    job_id: reference.job_id,
    interaction_id: reference.interaction_id,
    callback_type: typeof payload.type === "string" ? payload.type : "unknown",
    action_id: typeof payload.actions?.[0]?.action_id === "string" ? payload.actions[0].action_id : null,
    interaction_state: state,
  }));
}

/** Opens best-effort feedback for an unavailable Respond action while preserving Slack's acknowledgement. */
async function showUnavailableInteraction(env: Env, reference: InteractionReference, payload: Record<string, any>, state: "resolved" | "expired" | "missing"): Promise<void> {
  logIgnoredInteraction(reference, payload, state);
  if (payload.type !== "block_actions" || payload.actions?.[0]?.action_id !== "elicitation_open" || typeof payload.trigger_id !== "string") return;
  try {
    await slackAPI(env, "views.open", { trigger_id: payload.trigger_id, view: unavailableModal(state) });
  } catch (error) {
    console.error(JSON.stringify({
      level: "error",
      source: "worker",
      event: "slack_interaction_feedback_failed",
      job_id: reference.job_id,
      interaction_id: reference.interaction_id,
      interaction_state: state,
      error: error instanceof Error ? error.message : String(error),
    }));
  }
}

async function slackAPI<T extends Record<string, unknown>>(env: Env, method: string, payload: object): Promise<T> {
  const config = configuredSlack();
  const token = config && secret(env, config.botTokenSecret);
  if (!token) throw new Error("Slack bot token is unavailable");
  const response = await fetch(`https://slack.com/api/${method}`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}`, "content-type": "application/json; charset=utf-8" },
    body: JSON.stringify(payload),
  });
  const result = await response.json().catch(() => null) as ({ ok?: unknown; error?: unknown } & T) | null;
  if (!response.ok || result?.ok !== true) {
    const retryAfter = Number(response.headers.get("retry-after"));
    throw new SlackAPIError(
      `Slack ${method} failed${typeof result?.error === "string" ? `: ${result.error}` : ""}`,
      Number.isFinite(retryAfter) && retryAfter > 0 ? Math.ceil(retryAfter) : null,
    );
  }
  return result;
}

/** Delivers one durable elicitation notice to the configured Slack conversation. */
export async function postSlackElicitation(
  env: Env,
  reference: InteractionReference,
  message: string,
  origin: InteractionOrigin,
): Promise<SlackInteractionDelivery> {
  const config = configuredSlack();
  if (!config) throw new Error("Slack interactions are not configured");
  const result = await slackAPI<{ channel?: unknown; ts?: unknown }>(env, "chat.postMessage", {
    channel: config.conversationId,
    text: "An agent is waiting for input.",
    link_names: false,
    blocks: [
      { type: "section", text: { type: "plain_text", text: message.slice(0, 3000) } },
      { type: "actions", elements: [
        { type: "button", style: "primary", action_id: "elicitation_open", text: { type: "plain_text", text: "Respond" }, value: JSON.stringify(reference) },
        { type: "button", action_id: "elicitation_decline", text: { type: "plain_text", text: "Decline" }, value: JSON.stringify(reference), confirm: { title: { type: "plain_text", text: "Decline request?" }, text: { type: "mrkdwn", text: "The agent will resume without the requested input." }, confirm: { type: "plain_text", text: "Decline" }, deny: { type: "plain_text", text: "Go back" } } },
      ] },
      originFootnote(origin),
    ],
  });
  if (typeof result.channel !== "string" || result.channel !== config.conversationId || typeof result.ts !== "string" || !/^\d+\.\d+$/.test(result.ts)) {
    throw new Error("Slack chat.postMessage returned an invalid message reference");
  }
  return { channel: result.channel, messageTs: result.ts };
}

/** Validates a persisted Slack message reference before using it in an API call. */
export function parseSlackInteractionDelivery(value: unknown): SlackInteractionDelivery | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const delivery = value as Record<string, unknown>;
  const config = configuredSlack();
  return config && typeof delivery.channel === "string" && delivery.channel === config.conversationId &&
    typeof delivery.messageTs === "string" && /^\d+\.\d+$/.test(delivery.messageTs)
    ? { channel: delivery.channel, messageTs: delivery.messageTs }
    : null;
}

function outcomeText(outcome: InteractionOutcome): { fallback: string; status: string } {
  switch (outcome.status) {
    case "answered":
      return { fallback: `Agent input request answered at ${new Date(outcome.resolvedAt).toISOString()}.`, status: `:white_check_mark: Answered by <@${outcome.actorId}> · ${slackDate(outcome.resolvedAt)}` };
    case "declined":
      return { fallback: `Agent input request declined at ${new Date(outcome.resolvedAt).toISOString()}.`, status: `:no_entry_sign: Declined by <@${outcome.actorId}> · ${slackDate(outcome.resolvedAt)}` };
    case "cancelled":
      return { fallback: `Agent input request cancelled at ${new Date(outcome.resolvedAt).toISOString()}.`, status: `:white_circle: Cancelled by <@${outcome.actorId}> · ${slackDate(outcome.resolvedAt)}` };
    case "expired":
      return { fallback: "Agent input request expired.", status: ":hourglass_flowing_sand: Expired without a response" };
    case "run_cancelled":
      return { fallback: "Agent input request cancelled with its agent run.", status: ":no_entry_sign: Cancelled because the agent run was cancelled" };
  }
}

function slackDate(timestamp: number): string {
  const fallback = new Date(timestamp).toISOString();
  return `<!date^${Math.floor(timestamp / 1000)}^{date_short_pretty} at {time}|${fallback}>`;
}

/** Builds a completed Slack message without exposing submitted form values. */
export function completedSlackElicitation(
  message: string,
  outcome: InteractionOutcome,
  origin?: InteractionOrigin,
): { text: string; blocks: Record<string, unknown>[] } {
  const rendered = outcomeText(outcome);
  return {
    text: rendered.fallback,
    blocks: [
      { type: "section", text: { type: "plain_text", text: message.slice(0, 3000) } },
      { type: "context", elements: [{ type: "mrkdwn", text: rendered.status }] },
      ...(origin ? [originFootnote(origin)] : []),
    ],
  };
}

/** Replaces an elicitation's controls with its terminal status. */
export async function updateSlackElicitation(
  env: Env,
  delivery: SlackInteractionDelivery,
  message: string,
  outcome: InteractionOutcome,
  origin?: InteractionOrigin,
): Promise<void> {
  await slackAPI(env, "chat.update", {
    channel: delivery.channel,
    ts: delivery.messageTs,
    ...completedSlackElicitation(message, outcome, origin),
  });
}

function parseReference(value: unknown): InteractionReference | null {
  try {
    const parsed = JSON.parse(String(value)) as Record<string, unknown>;
    return typeof parsed.job_id === "string" && /^[0-9a-f-]{36}$/.test(parsed.job_id) && typeof parsed.interaction_id === "string" && /^[0-9a-f-]{36}$/.test(parsed.interaction_id)
      ? { job_id: parsed.job_id, interaction_id: parsed.interaction_id }
      : null;
  } catch { return null; }
}

function submittedContent(request: ElicitationRequest, state: unknown): Record<string, unknown> {
  const values = state && typeof state === "object" && !Array.isArray(state) ? (state as { values?: unknown }).values : null;
  if (!values || typeof values !== "object" || Array.isArray(values)) throw new Error("Slack form state is invalid");
  const result: Record<string, unknown> = {};
  for (const [name, property] of Object.entries(request.requestedSchema.properties)) {
    const block = (values as Record<string, unknown>)[`block:${name}`];
    const raw = block && typeof block === "object" && !Array.isArray(block) ? (block as Record<string, unknown>)[`field:${name}`] : null;
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) continue;
    const input = raw as Record<string, unknown>;
    if (property.type === "array") result[name] = Array.isArray(input.selected_options) ? input.selected_options.map((item) => (item as { value?: unknown }).value).filter((value): value is string => typeof value === "string") : [];
    else if (property.type === "boolean") result[name] = Array.isArray(input.selected_options) && input.selected_options.length > 0;
    else {
      const value = typeof input.value === "string" ? input.value : (input.selected_option as { value?: unknown } | undefined)?.value;
      if (typeof value !== "string" || value === "") continue;
      result[name] = property.type === "number" || property.type === "integer" ? Number(value) : value;
    }
  }
  return result;
}

/** Authenticates and handles Slack actions without exposing them through bearer auth. */
export async function handleSlackInteraction(request: Request, env: Env): Promise<Response> {
  const config = configuredSlack();
  if (!config) return apiError(404, "not_found", "Not found");
  const body = await request.text();
  if (encoder.encode(body).byteLength > config.maxResponseBytes) return apiError(413, "payload_too_large", "Slack payload is too large");
  const signingSecret = secret(env, config.signingSecret);
  if (!signingSecret || !(await verifySlackRequest(request, body, signingSecret))) return apiError(401, "unauthorized", "Slack signature is invalid");
  const encoded = new URLSearchParams(body).get("payload");
  let payload: Record<string, any>;
  try { payload = JSON.parse(encoded ?? ""); } catch { return apiError(400, "invalid_request", "Slack payload is invalid"); }
  if (payload.team?.id !== config.teamId || typeof payload.user?.id !== "string" || !config.allowedUserIds.includes(payload.user.id)) return apiError(403, "forbidden", "Slack actor is not allowed");
  if (payload.type === "block_actions" && payload.channel?.id !== config.conversationId) return apiError(403, "forbidden", "Slack conversation is not allowed");

  const rawReference = payload.type === "view_submission" || payload.type === "view_closed"
    ? payload.view?.private_metadata
    : payload.actions?.[0]?.value;
  const reference = parseReference(rawReference);
  if (!reference) return apiError(400, "invalid_request", "Slack interaction reference is invalid");
  const stub = env.AGENT_CONTAINER.get(env.AGENT_CONTAINER.idFromName(reference.job_id)) as unknown as SlackInteractionStub;
  if (payload.type === "block_actions" && payload.actions?.[0]?.action_id === "elicitation_decline") {
    const resolved = await stub.resolveInteractionFromProvider(reference.interaction_id, payload.user.id, { action: "decline" });
    if (resolved.state === "forbidden") return apiError(403, "forbidden", "Slack actor is not allowed");
    if (!resolved.accepted) logIgnoredInteraction(reference, payload, resolved.state);
    return slackAck();
  }
  const interaction = await stub.getInteractionForProvider(reference.interaction_id, payload.user.id);
  if (interaction.state === "forbidden") return apiError(403, "forbidden", "Slack actor is not allowed");
  if (interaction.state !== "pending") {
    const supportedUnavailableAction =
      payload.type === "view_submission" || payload.type === "view_closed" ||
      payload.type === "block_actions" && payload.actions?.[0]?.action_id === "elicitation_open";
    if (!supportedUnavailableAction) return apiError(400, "invalid_request", "Unsupported Slack interaction payload");
    await showUnavailableInteraction(env, reference, payload, interaction.state);
    return slackAck();
  }
  if (payload.type === "block_actions" && payload.actions?.[0]?.action_id === "elicitation_open" && typeof payload.trigger_id === "string") {
    await slackAPI(env, "views.open", { trigger_id: payload.trigger_id, view: modal(reference, interaction.request) });
    return slackAck();
  }
  if (payload.type === "view_closed") {
    const resolved = await stub.resolveInteractionFromProvider(reference.interaction_id, payload.user.id, { action: "cancel" });
    if (resolved.state === "forbidden") return apiError(403, "forbidden", "Slack actor is not allowed");
    if (!resolved.accepted) logIgnoredInteraction(reference, payload, resolved.state);
    return slackAck();
  }
  if (payload.type === "view_submission") {
    const content = submittedContent(interaction.request, payload.view?.state);
    const validationError = validateElicitationContent(interaction.request.requestedSchema, content);
    if (validationError) return json({ response_action: "errors", errors: { [Object.keys(payload.view?.state?.values ?? {})[0] ?? ""]: validationError } });
    const resolved = await stub.resolveInteractionFromProvider(reference.interaction_id, payload.user.id, { action: "accept", content });
    if (resolved.validationError) return json({ response_action: "errors", errors: { [Object.keys(payload.view?.state?.values ?? {})[0] ?? ""]: resolved.validationError } });
    if (resolved.state === "forbidden") return apiError(403, "forbidden", "Slack actor is not allowed");
    if (!resolved.accepted) logIgnoredInteraction(reference, payload, resolved.state);
    return slackAck();
  }
  return apiError(400, "invalid_request", "Unsupported Slack interaction payload");
}
