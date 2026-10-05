import { appConfig } from "./generated-config.ts";
import {
  parseSlackInteractionDelivery,
  postSlackElicitation,
  SlackAPIError,
  updateSlackElicitation,
  type SlackInteractionDelivery,
} from "./slack-interactions.ts";
import type { InteractionOrigin, InteractionOutcome } from "./interactions.ts";
import type { Env } from "./types.ts";

export interface InteractionReference { job_id: string; interaction_id: string }
export type InteractionDelivery = { provider: "slack"; reference: SlackInteractionDelivery; origin?: InteractionOrigin };
const interactionName = /^[a-z][a-z0-9_-]{0,63}$/;

function parseInteractionOrigin(value: unknown): InteractionOrigin | undefined {
  if (!value || typeof value !== "object" || Array.isArray(value)) return undefined;
  const origin = value as Record<string, unknown>;
  if (typeof origin.workflow !== "string" || !interactionName.test(origin.workflow) ||
      typeof origin.stepId !== "string" || !interactionName.test(origin.stepId)) return undefined;
  return { workflow: origin.workflow, stepId: origin.stepId };
}

/** Dispatches a generic interaction through the configured chat provider. */
export async function deliverInteraction(env: Env, reference: InteractionReference, message: string, origin: InteractionOrigin): Promise<InteractionDelivery> {
  switch (appConfig.interactions.provider) {
    case "slack":
      return { provider: "slack", reference: await postSlackElicitation(env, reference, message, origin), origin };
    case "none":
      throw new Error("Human interactions are not configured");
  }
}

/** Validates a persisted provider delivery reference. */
export function parseInteractionDelivery(value: unknown): InteractionDelivery | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const delivery = value as Record<string, unknown>;
  if (delivery.provider !== "slack") return null;
  const reference = parseSlackInteractionDelivery(delivery.reference);
  const origin = parseInteractionOrigin(delivery.origin);
  return reference ? { provider: "slack", reference, ...(origin ? { origin } : {}) } : null;
}

/** Replaces a provider interaction message with its terminal outcome. */
export async function completeInteraction(
  env: Env,
  delivery: InteractionDelivery,
  message: string,
  outcome: InteractionOutcome,
): Promise<void> {
  switch (delivery.provider) {
    case "slack":
      return updateSlackElicitation(env, delivery.reference, message, outcome, delivery.origin);
  }
}

/** Returns a provider-requested retry delay when one is available. */
export function interactionRetryAfter(error: unknown): number | null {
  return error instanceof SlackAPIError ? error.retryAfterSeconds : null;
}

/** Applies provider-specific actor authorization behind a stable broker boundary. */
export function interactionActorAllowed(actorId: string): boolean {
  switch (appConfig.interactions.provider) {
    case "slack":
      return appConfig.interactions.allowedUserIds.includes(actorId);
    case "none":
      return false;
  }
}
