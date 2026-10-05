export type InteractionAction = "accept" | "decline" | "cancel";

export interface ElicitationProperty {
  type: "string" | "number" | "integer" | "boolean" | "array";
  title?: string;
  description?: string;
  default?: string | number | boolean | string[];
  minLength?: number;
  maxLength?: number;
  minimum?: number;
  maximum?: number;
  pattern?: string;
  format?: string;
  enum?: string[];
  oneOf?: Array<{ const: string; title?: string; description?: string }>;
  items?: { type?: "string"; enum?: string[]; anyOf?: Array<{ const: string; title?: string; description?: string }> };
  minItems?: number;
  maxItems?: number;
  _meta?: Record<string, unknown>;
}

export interface ElicitationSchema {
  type: "object";
  title?: string;
  description?: string;
  properties: Record<string, ElicitationProperty>;
  required?: string[];
}

export interface ElicitationRequest {
  mode: "form";
  message: string;
  requestedSchema: ElicitationSchema;
}

export interface InteractionResponse {
  action: InteractionAction;
  content?: Record<string, unknown>;
}

export interface InteractionOrigin {
  workflow: string;
  stepId: string;
}

export type InteractionProviderState = "pending" | "resolved" | "expired" | "missing" | "forbidden";

export type InteractionProviderLookup =
  | { state: "pending"; request: ElicitationRequest }
  | { state: Exclude<InteractionProviderState, "pending"> };

export interface InteractionProviderResolution {
  accepted: boolean;
  state: InteractionProviderState;
  validationError?: string;
}

/** Returns whether a resolved response can continue inside the active harness prompt. */
export function interactionContinuesCurrentPrompt(response: InteractionResponse): boolean {
  return response.action === "accept";
}

export type InteractionOutcome =
  | { status: "answered" | "declined" | "cancelled"; actorId: string; resolvedAt: number }
  | { status: "expired" | "run_cancelled" };

export interface PendingInteraction {
  id: string;
  mode: "form";
  message: string;
  created_at: string;
  expires_at: string;
}

const propertyName = /^[A-Za-z_][A-Za-z0-9_.-]{0,63}$/;
const maximumQuestions = 5;
const maximumFormProperties = maximumQuestions * 2;

function optionalString(value: unknown, maximum: number): value is string | undefined {
  return value === undefined || (typeof value === "string" && value.length <= maximum);
}

function stringChoices(value: unknown): value is string[] | undefined {
  return value === undefined || (Array.isArray(value) && value.length > 0 && value.length <= 100 &&
    value.every((item) => typeof item === "string" && item.length > 0 && item.length <= 75) &&
    new Set(value).size === value.length);
}

function titledChoices(value: unknown): value is Array<{ const: string; title?: string; description?: string }> | undefined {
  return value === undefined || (Array.isArray(value) && value.length > 0 && value.length <= 100 && value.every((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return false;
    const choice = item as Record<string, unknown>;
    return typeof choice.const === "string" && choice.const.length > 0 && choice.const.length <= 75 &&
      optionalString(choice.title, 75) && optionalString(choice.description, 1000);
  }) && new Set(value.map((item) => (item as { const: string }).const)).size === value.length);
}

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : null;
}

/** Returns the primary field named by a recognized native custom-answer companion. */
function customAnswerTarget(property: ElicitationProperty): string | null {
  const metadata = record(property._meta);
  if (!metadata) return null;
  const shared = metadata._askUserQuestionCustomAnswer;
  if (shared !== undefined) {
    const marker = record(shared);
    if (!marker || marker.isCustomAnswer !== true || typeof marker.questionId !== "string" || !propertyName.test(marker.questionId)) {
      throw new Error("custom-answer metadata is invalid");
    }
    return marker.questionId;
  }
  const codex = metadata.codex;
  if (codex !== undefined) {
    const marker = record(codex);
    if (marker?.isOtherAnswer === true) {
      if (typeof marker.questionId !== "string" || !propertyName.test(marker.questionId)) throw new Error("custom-answer metadata is invalid");
      return marker.questionId;
    }
  }
  return null;
}

/** Parses the bounded, form-only ACP elicitation subset rendered by chat providers. */
export function parseElicitationRequest(value: unknown): ElicitationRequest {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("elicitation must be an object");
  const request = value as Record<string, unknown>;
  if (request.mode !== "form" || typeof request.message !== "string" || request.message.trim().length === 0 || request.message.length > 3000) {
    throw new Error("elicitation mode or message is invalid");
  }
  const rawSchema = request.requestedSchema;
  if (!rawSchema || typeof rawSchema !== "object" || Array.isArray(rawSchema)) throw new Error("requestedSchema must be an object");
  const schema = rawSchema as Record<string, unknown>;
  if (schema.type !== undefined && schema.type !== "object") throw new Error("requestedSchema.type must be object");
  if (!schema.properties || typeof schema.properties !== "object" || Array.isArray(schema.properties)) throw new Error("requestedSchema.properties must be an object");
  const entries = Object.entries(schema.properties as Record<string, unknown>);
  if (entries.length === 0 || entries.length > maximumFormProperties) throw new Error("requestedSchema must contain 1 to 5 questions");
  const properties: Record<string, ElicitationProperty> = {};
  for (const [name, raw] of entries) {
    if (!propertyName.test(name) || !raw || typeof raw !== "object" || Array.isArray(raw)) throw new Error(`property ${name} is invalid`);
    const item = raw as Record<string, unknown>;
    if (!(item.type === "string" || item.type === "number" || item.type === "integer" || item.type === "boolean" || item.type === "array")) throw new Error(`property ${name} has an unsupported type`);
    if (!optionalString(item.title, 200) || !optionalString(item.description, 1000) || !optionalString(item.pattern, 256) || !optionalString(item.format, 64)) throw new Error(`property ${name} metadata is invalid`);
    for (const key of ["minLength", "maxLength", "minItems", "maxItems"] as const) {
      if (item[key] !== undefined && (!Number.isSafeInteger(item[key]) || (item[key] as number) < 0 || (item[key] as number) > 3000)) throw new Error(`property ${name}.${key} is invalid`);
    }
    for (const key of ["minimum", "maximum"] as const) {
      if (item[key] !== undefined && (typeof item[key] !== "number" || !Number.isFinite(item[key]))) throw new Error(`property ${name}.${key} is invalid`);
    }
    if (!stringChoices(item.enum) || !titledChoices(item.oneOf)) throw new Error(`property ${name} choices are invalid`);
    if (item.type === "array") {
      if (!item.items || typeof item.items !== "object" || Array.isArray(item.items)) throw new Error(`property ${name}.items is invalid`);
      const items = item.items as Record<string, unknown>;
      if (items.type !== undefined && items.type !== "string") throw new Error(`property ${name}.items.type is invalid`);
      if (!stringChoices(items.enum) || !titledChoices(items.anyOf) || (!items.enum && !items.anyOf)) throw new Error(`property ${name}.items choices are invalid`);
    }
    properties[name] = item as unknown as ElicitationProperty;
  }
  const companions = new Map<string, string>();
  for (const [name, property] of Object.entries(properties)) {
    const target = customAnswerTarget(property);
    if (target === null) continue;
    const primary = properties[target];
    if (name === target || property.type !== "string" || !primary || companions.has(target) || customAnswerTarget(primary) !== null ||
      !(primary.oneOf || primary.items?.anyOf)) throw new Error(`property ${name} has invalid custom-answer metadata`);
    companions.set(target, name);
  }
  const questionCount = entries.length - companions.size;
  if (questionCount < 1 || questionCount > maximumQuestions) throw new Error("requestedSchema must contain 1 to 5 questions");
  const required = schema.required;
  if (required !== undefined && (!Array.isArray(required) || required.some((name) => typeof name !== "string" || !(name in properties)) || new Set(required).size !== required.length)) {
    throw new Error("requestedSchema.required is invalid");
  }
  return {
    mode: "form",
    message: request.message,
    requestedSchema: {
      type: "object",
      title: optionalString(schema.title, 200) ? schema.title : undefined,
      description: optionalString(schema.description, 1000) ? schema.description : undefined,
      properties,
      required: required as string[] | undefined,
    },
  };
}

function choices(property: ElicitationProperty): string[] | null {
  if (property.enum) return property.enum;
  if (property.oneOf) return property.oneOf.map((item) => item.const);
  if (property.items?.enum) return property.items.enum;
  if (property.items?.anyOf) return property.items.anyOf.map((item) => item.const);
  return null;
}

/** Revalidates provider-submitted content against the ACP primitive schema. */
export function validateElicitationContent(schema: ElicitationSchema, content: Record<string, unknown>): string | null {
  const required = new Set(schema.required ?? []);
  for (const key of Object.keys(content)) if (!(key in schema.properties)) return `Unknown field: ${key}`;
  for (const [name, property] of Object.entries(schema.properties)) {
    const value = content[name];
    if (value === undefined || value === null || value === "") {
      if (required.has(name)) return `${property.title ?? name} is required`;
      continue;
    }
    if (property.type === "string") {
      if (typeof value !== "string") return `${property.title ?? name} must be text`;
      if (property.minLength !== undefined && value.length < property.minLength) return `${property.title ?? name} is too short`;
      if (property.maxLength !== undefined && value.length > property.maxLength) return `${property.title ?? name} is too long`;
      if (property.pattern) {
        try { if (!new RegExp(property.pattern).test(value)) return `${property.title ?? name} has an invalid format`; }
        catch { return `${property.title ?? name} has an invalid pattern`; }
      }
    } else if (property.type === "number" || property.type === "integer") {
      if (typeof value !== "number" || !Number.isFinite(value) || (property.type === "integer" && !Number.isInteger(value))) return `${property.title ?? name} must be a ${property.type}`;
      if (property.minimum !== undefined && value < property.minimum) return `${property.title ?? name} is too small`;
      if (property.maximum !== undefined && value > property.maximum) return `${property.title ?? name} is too large`;
    } else if (property.type === "boolean") {
      if (typeof value !== "boolean") return `${property.title ?? name} must be true or false`;
    } else if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
      return `${property.title ?? name} must be a list`;
    }
    const allowed = choices(property);
    if (allowed && (Array.isArray(value) ? value.some((item) => !allowed.includes(item)) : !allowed.includes(String(value)))) return `${property.title ?? name} contains an invalid choice`;
    if (Array.isArray(value) && property.minItems !== undefined && value.length < property.minItems) return `${property.title ?? name} has too few choices`;
    if (Array.isArray(value) && property.maxItems !== undefined && value.length > property.maxItems) return `${property.title ?? name} has too many choices`;
  }
  return null;
}
