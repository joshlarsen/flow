import { lstat, readFile } from "node:fs/promises";
import path from "node:path";
import { createHash } from "node:crypto";
import { parse as parseEnv } from "dotenv";
import { parse as parseYaml } from "yaml";
import { z } from "zod";
import {
  resolveEnvironmentFiles,
  type EnvironmentProfile,
} from "./environment-files.ts";

const durationPattern = /^\d+(?:ms|s|m|h|d)$/;
const secretNamePattern = /^[A-Z][A-Z0-9_]*$/;
const namePattern = /^[a-z][a-z0-9_-]{0,63}$/;
const accountIdPattern = /^[0-9a-f]{32}$/i;
const headerNamePattern = /^[!#$%&'*+.^_`|~0-9A-Za-z-]+$/;
const oauthParameterNamePattern = /^[A-Za-z][A-Za-z0-9._~-]{0,63}$/;
const oauthScopePattern = /^[\u0021\u0023-\u005b\u005d-\u007e]+$/;
const reservedOAuthParameters = new Set([
  "grant_type",
  "client_id",
  "client_secret",
  "refresh_token",
  "scope",
]);
export const basicAuthValuePrefix = "Basic ";
const duration = z
  .string()
  .regex(
    durationPattern,
    "must be an integer duration such as 10s, 15m, or 24h",
  );
const executionDuration = duration.refine((value) => {
  const milliseconds = durationToMilliseconds(value);
  return Number.isSafeInteger(milliseconds) && milliseconds > 0;
}, "must be a positive duration representable in milliseconds");
const name = z.string().regex(namePattern);
const modelName = z.string().regex(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/);
const httpsUrl = z
  .string()
  .url()
  .superRefine((value, ctx) => {
    const url = new URL(value);
    if (url.protocol !== "https:")
      ctx.addIssue({ code: "custom", message: "must use HTTPS" });
    if (url.username || url.password)
      ctx.addIssue({ code: "custom", message: "must not contain credentials" });
    if (url.search || url.hash)
      ctx.addIssue({
        code: "custom",
        message: "must not contain a query or fragment",
      });
  });
const promptPath = z
  .string()
  .min(4)
  .max(255)
  .superRefine((value, ctx) => {
    if (
      value.startsWith("/") ||
      value.includes("\\") ||
      !value.endsWith(".md")
    ) {
      ctx.addIssue({ code: "custom", message: "must be a relative .md path" });
      return;
    }
    const parts = value.split("/");
    if (
      parts.some(
        (part) =>
          part === "" || part === "." || part === ".." || part.startsWith("."),
      )
    ) {
      ctx.addIssue({
        code: "custom",
        message: "must not contain empty, dot, or hidden path segments",
      });
    }
  });

const cronAliases = {
  month: new Map([
    ["JAN", 1],
    ["FEB", 2],
    ["MAR", 3],
    ["APR", 4],
    ["MAY", 5],
    ["JUN", 6],
    ["JUL", 7],
    ["AUG", 8],
    ["SEP", 9],
    ["OCT", 10],
    ["NOV", 11],
    ["DEC", 12],
  ]),
  weekday: new Map([
    ["SUN", 1],
    ["MON", 2],
    ["TUE", 3],
    ["WED", 4],
    ["THU", 5],
    ["FRI", 6],
    ["SAT", 7],
  ]),
} as const;

function cronValue(
  input: string,
  minimum: number,
  maximum: number,
  aliases?: ReadonlyMap<string, number>,
): number | null {
  const aliased = aliases?.get(input.toUpperCase());
  if (aliased !== undefined) return aliased;
  if (!/^\d{1,2}$/.test(input)) return null;
  const value = Number(input);
  return value >= minimum && value <= maximum ? value : null;
}

/** Validates one ordinary numeric/name cron field without calendar extensions. */
function validCronField(
  input: string,
  minimum: number,
  maximum: number,
  aliases?: ReadonlyMap<string, number>,
): boolean {
  return input.split(",").every((part) => {
    const [base, step, ...extra] = part.split("/");
    if (base === undefined) return false;
    if (
      extra.length > 0 ||
      (step !== undefined &&
        (!/^\d{1,2}$/.test(step) || Number(step) < 1 || Number(step) > maximum))
    )
      return false;
    if (base === "*") return true;
    const range = base.split("-");
    if (range.length === 1)
      return cronValue(range[0]!, minimum, maximum, aliases) !== null;
    if (range.length !== 2) return false;
    const start = cronValue(range[0]!, minimum, maximum, aliases);
    const end = cronValue(range[1]!, minimum, maximum, aliases);
    return start !== null && end !== null && start <= end;
  });
}

/** Implements the five-field Cloudflare Cron grammar used for local preflight validation. */
export function isCloudflareCronExpression(input: string): boolean {
  if (input.length > 256 || !/^[^\s]+ [^\s]+ [^\s]+ [^\s]+ [^\s]+$/.test(input))
    return false;
  const [minute, hour, dayOfMonth, month, weekday] = input.split(" ");
  if (
    !validCronField(minute!, 0, 59) ||
    !validCronField(hour!, 0, 23) ||
    !validCronField(month!, 1, 12, cronAliases.month)
  )
    return false;
  const validDayOfMonth =
    dayOfMonth === "L" ||
    dayOfMonth === "LW" ||
    /^(?:[1-9]|[12]\d|3[01])W$/.test(dayOfMonth!) ||
    validCronField(dayOfMonth!, 1, 31);
  const validWeekday =
    weekday === "L" ||
    /^(?:(?:[1-7])|(?:SUN|MON|TUE|WED|THU|FRI|SAT))L$/i.test(weekday!) ||
    (() => {
      const match = /^(.+)#([1-5])$/.exec(weekday!);
      return match
        ? cronValue(match[1]!, 1, 7, cronAliases.weekday) !== null
        : validCronField(weekday!, 1, 7, cronAliases.weekday);
    })();
  return validDayOfMonth && validWeekday;
}

const providerProtocol = z.enum([
  "xai",
  "openai-responses",
  "anthropic",
  "openai-compatible",
]);
const providerEndpoint = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("url"), base_url: httpsUrl }).strict(),
  z.object({ kind: z.literal("cloudflare-ai") }).strict(),
]);
const staticHeaders = z
  .record(z.string().regex(headerNamePattern), z.string().max(1024))
  .default({});

/** Reserves the canonical Basic prefix for Authorization-header transformation. */
function credentialValuePrefixError(
  header: string,
  valuePrefix: string,
): string | undefined {
  if (valuePrefix.trim().toLowerCase() !== "basic") return undefined;
  if (valuePrefix !== basicAuthValuePrefix)
    return `Basic authentication must use the exact value_prefix ${JSON.stringify(basicAuthValuePrefix)}`;
  if (header.toLowerCase() !== "authorization")
    return "Basic authentication must use the Authorization header";
  return undefined;
}

const credentialSource = z
  .object({
    env: z.string().regex(secretNamePattern).optional(),
    header: z.string().regex(headerNamePattern),
    value_prefix: z.string().max(128).default(""),
  })
  .strict()
  .superRefine((value, ctx) => {
    const error = credentialValuePrefixError(value.header, value.value_prefix);
    if (error)
      ctx.addIssue({ code: "custom", path: ["value_prefix"], message: error });
  });
const staticCredentialUpstream = z
  .object({
    header: z.string().regex(headerNamePattern),
    secret: z.string().regex(secretNamePattern),
    value_prefix: z.string().max(128).default(""),
  })
  .strict()
  .superRefine((value, ctx) => {
    const error = credentialValuePrefixError(value.header, value.value_prefix);
    if (error)
      ctx.addIssue({ code: "custom", path: ["value_prefix"], message: error });
  });
const oauthClientAuthentication = z.discriminatedUnion("method", [
  z
    .object({
      method: z.enum(["client_secret_basic", "client_secret_post"]),
      secret: z.string().regex(secretNamePattern),
    })
    .strict(),
  z.object({ method: z.literal("none") }).strict(),
]);
const oauthGrant = z.discriminatedUnion("type", [
  z.object({ type: z.literal("client_credentials") }).strict(),
  z
    .object({
      type: z.literal("refresh_token"),
      refresh_token_secret: z.string().regex(secretNamePattern),
    })
    .strict(),
]);
const oauthConfig = z
  .object({
    token_url: httpsUrl,
    client_id_secret: z.string().regex(secretNamePattern),
    client_auth: oauthClientAuthentication,
    grant: oauthGrant,
    scopes: z
      .array(
        z
          .string()
          .min(1)
          .max(256)
          .regex(
            oauthScopePattern,
            "must be one OAuth scope token without spaces or control characters",
          ),
      )
      .max(64)
      .default([]),
    extra_parameters: z
      .record(
        z.string().regex(oauthParameterNamePattern),
        z
          .string()
          .max(2048)
          .refine(
            (value) => !/[\u0000-\u001f\u007f]/u.test(value),
            "must not contain control characters",
          ),
      )
      .default({}),
    fallback_ttl: executionDuration.optional(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (
      value.grant.type === "client_credentials" &&
      value.client_auth.method === "none"
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["client_auth", "method"],
        message:
          "client_credentials requires confidential client authentication",
      });
    }
    for (const parameter of Object.keys(value.extra_parameters)) {
      if (reservedOAuthParameters.has(parameter)) {
        ctx.addIssue({
          code: "custom",
          path: ["extra_parameters", parameter],
          message: "is managed by the OAuth credential configuration",
        });
      }
    }
    if (Object.keys(value.extra_parameters).length > 32) {
      ctx.addIssue({
        code: "custom",
        path: ["extra_parameters"],
        message: "must contain at most 32 parameters",
      });
    }
    if (new Set(value.scopes).size !== value.scopes.length) {
      ctx.addIssue({
        code: "custom",
        path: ["scopes"],
        message: "must not contain duplicates",
      });
    }
  });
const oauthCredentialUpstream = z.object({ oauth: oauthConfig }).strict();
const credentialUpstream = z.union([
  staticCredentialUpstream,
  oauthCredentialUpstream,
]);
const credentialSchema = z
  .object({ source: credentialSource, upstream: credentialUpstream })
  .strict();
const providerSchema = z
  .object({
    protocol: providerProtocol,
    endpoint: providerEndpoint,
    credential: name,
    static_headers: staticHeaders,
  })
  .strict();
const modelSchema = z
  .object({ providers: z.record(name, z.string().min(1)) })
  .strict();
const harnessSchema = z
  .object({ type: z.enum(["codex", "grok", "claude-code", "opencode", "pi"]) })
  .strict();
const routeSchema = z
  .object({ url_prefix: httpsUrl, credential: name })
  .strict();
const environmentVariableSchema = z
  .object({ type: z.literal("plaintext") })
  .strict();
const interactionLimitsSchema = z
  .object({
    live_wait: executionDuration.default("30s"),
    response_ttl: executionDuration.default("24h"),
    max_request_bytes: z
      .number()
      .int()
      .min(1024)
      .max(1024 * 1024)
      .default(65_536),
    max_response_bytes: z
      .number()
      .int()
      .min(1024)
      .max(1024 * 1024)
      .default(65_536),
    checkpoint: z
      .object({
        max_files: z.number().int().min(1).max(100_000).default(10_000),
        max_file_bytes: z
          .number()
          .int()
          .min(1)
          .max(1024 * 1024 * 1024)
          .default(134_217_728),
        max_total_bytes: z
          .number()
          .int()
          .min(1)
          .max(2 * 1024 * 1024 * 1024)
          .default(268_435_456),
      })
      .strict()
      .default({
        max_files: 10_000,
        max_file_bytes: 134_217_728,
        max_total_bytes: 268_435_456,
      }),
  })
  .strict();
const slackInteractionsSchema = interactionLimitsSchema
  .extend({
    provider: z.literal("slack"),
    team_id: z.string().regex(/^T[A-Z0-9]{1,31}$/),
    conversation_id: z.string().regex(/^[CGD][A-Z0-9]{1,31}$/),
    allowed_user_ids: z
      .array(z.string().regex(/^U[A-Z0-9]{1,31}$/))
      .min(1)
      .max(100),
    bot_token_secret: z.string().regex(secretNamePattern),
    signing_secret: z.string().regex(secretNamePattern),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (
      new Set(value.allowed_user_ids).size !== value.allowed_user_ids.length
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["allowed_user_ids"],
        message: "must not contain duplicates",
      });
    }
    const liveWaitMs = durationToMilliseconds(value.live_wait);
    const responseTtlMs = durationToMilliseconds(value.response_ttl);
    if (liveWaitMs > 60_000)
      ctx.addIssue({
        code: "custom",
        path: ["live_wait"],
        message: "must not exceed 60s",
      });
    if (responseTtlMs < liveWaitMs || responseTtlMs > 7 * 24 * 60 * 60_000)
      ctx.addIssue({
        code: "custom",
        path: ["response_ttl"],
        message: "must be between live_wait and 7d",
      });
    if (value.checkpoint.max_file_bytes > value.checkpoint.max_total_bytes)
      ctx.addIssue({
        code: "custom",
        path: ["checkpoint", "max_file_bytes"],
        message: "must not exceed max_total_bytes",
      });
  });
const interactionsSchema = z.discriminatedUnion("provider", [
  z.object({ provider: z.literal("none") }).strict(),
  slackInteractionsSchema,
]);

export const runnerSettingsSchema = z
  .object({
    version: z.literal(1),
    deployment: z
      .object({
        name: z.string().regex(/^[a-z][a-z0-9-]{0,62}$/),
        compatibility_date: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
        global_fetch_strictly_public: z.boolean().default(false),
      })
      .strict(),
    container: z
      .object({
        instance_type: z.enum([
          "lite",
          "basic",
          "standard-1",
          "standard-2",
          "standard-3",
          "standard-4",
        ]),
        max_instances: z.number().int().min(1).max(1000),
        port: z.number().int().min(1024).max(65535),
      })
      .strict(),
    egress: z
      .object({
        oauth: z
          .object({
            token_request_timeout: executionDuration.default("10s"),
            expiry_skew: executionDuration.default("30s"),
            max_token_request_bytes: z
              .number()
              .int()
              .min(1024)
              .max(1024 * 1024)
              .default(65_536),
            max_token_response_bytes: z
              .number()
              .int()
              .min(1024)
              .max(1024 * 1024)
              .default(65_536),
            max_token_bytes: z
              .number()
              .int()
              .min(256)
              .max(256 * 1024)
              .default(16_384),
          })
          .strict()
          .default({
            token_request_timeout: "10s",
            expiry_skew: "30s",
            max_token_request_bytes: 65_536,
            max_token_response_bytes: 65_536,
            max_token_bytes: 16_384,
          }),
      })
      .strict()
      .default({
        oauth: {
          token_request_timeout: "10s",
          expiry_skew: "30s",
          max_token_request_bytes: 65_536,
          max_token_response_bytes: 65_536,
          max_token_bytes: 16_384,
        },
      }),
    api: z
      .object({
        auth_secret: z.string().regex(secretNamePattern),
        retention: duration,
      })
      .strict(),
    history: z
      .object({
        max_verbose_bytes: z.number().int().min(1024).default(26214400),
        max_metric_bytes: z.number().int().min(1024).default(5242880),
        max_lifecycle_bytes: z.number().int().min(1024).default(1048576),
        max_pending_bytes: z.number().int().min(1024).default(8388608),
      })
      .strict(),
    runner: z
      .object({
        shutdown_grace: duration,
        max_prompt_bytes: z
          .number()
          .int()
          .min(1)
          .max(1024 * 1024),
        max_prompt_files: z.number().int().min(1).max(1000),
        max_prompt_bundle_bytes: z
          .number()
          .int()
          .min(1)
          .max(16 * 1024 * 1024),
        max_asset_bytes: z
          .number()
          .int()
          .min(1)
          .max(16 * 1024 * 1024),
        max_asset_files: z.number().int().min(1).max(2000),
        max_asset_bundle_bytes: z
          .number()
          .int()
          .min(1)
          .max(64 * 1024 * 1024),
        max_result_bytes: z
          .number()
          .int()
          .min(1024)
          .max(8 * 1024 * 1024),
      })
      .strict(),
    artifacts: z
      .object({
        max_files: z.number().int().min(1).max(1000),
        max_file_bytes: z
          .number()
          .int()
          .min(1)
          .max(100 * 1024 * 1024),
        max_total_bytes: z
          .number()
          .int()
          .min(1)
          .max(1024 * 1024 * 1024),
      })
      .strict(),
    memory: z
      .object({
        max_database_bytes: z
          .number()
          .int()
          .min(4096)
          .max(1024 * 1024 * 1024),
        persistence_timeout: executionDuration,
      })
      .strict(),
    interactions: interactionsSchema,
    observability: z
      .object({
        traces: z
          .object({
            enabled: z.literal(true),
            persist: z.literal(true),
            head_sampling_rate: z.number().min(0).max(1),
          })
          .strict(),
      })
      .strict(),
    logging: z
      .object({
        events: z.literal("full"),
        max_log_bytes: z.number().int().min(1024).max(240_000),
      })
      .strict(),
  })
  .strict()
  .superRefine((value, ctx) => {
    if (
      value.interactions.provider !== "none" &&
      !value.deployment.global_fetch_strictly_public
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["deployment", "global_fetch_strictly_public"],
        message:
          "must be true when interactions use the Worker's public callback URL",
      });
    }
    if (value.runner.max_prompt_bytes > value.runner.max_prompt_bundle_bytes) {
      ctx.addIssue({
        code: "custom",
        path: ["runner", "max_prompt_bytes"],
        message: "must not exceed max_prompt_bundle_bytes",
      });
    }
    if (value.runner.max_asset_bytes > value.runner.max_asset_bundle_bytes) {
      ctx.addIssue({
        code: "custom",
        path: ["runner", "max_asset_bytes"],
        message: "must not exceed max_asset_bundle_bytes",
      });
    }
    if (value.artifacts.max_file_bytes > value.artifacts.max_total_bytes) {
      ctx.addIssue({
        code: "custom",
        path: ["artifacts", "max_file_bytes"],
        message: "must not exceed max_total_bytes",
      });
    }
  });

export const catalogSchema = z
  .object({
    version: z.literal(1),
    environment: z
      .record(z.string().regex(secretNamePattern), environmentVariableSchema)
      .default({}),
    credentials: z.record(name, credentialSchema),
    providers: z
      .record(name, providerSchema)
      .refine(
        (value) => Object.keys(value).length > 0,
        "must define at least one provider",
      ),
    models: z
      .record(modelName, modelSchema)
      .refine(
        (value) => Object.keys(value).length > 0,
        "must define at least one model",
      ),
    harnesses: z
      .record(name, harnessSchema)
      .refine(
        (value) => Object.keys(value).length > 0,
        "must define at least one harness",
      ),
    routes: z.record(name, routeSchema).default({}),
  })
  .strict();

const commandArgument = z
  .string()
  .refine(
    (value) => new TextEncoder().encode(value).byteLength <= 4096,
    "must be at most 4096 UTF-8 bytes",
  );
const command = z
  .array(commandArgument)
  .min(1)
  .max(64)
  .superRefine((value, ctx) => {
    if (!value[0]?.trim())
      ctx.addIssue({
        code: "custom",
        path: [0],
        message: "executable must not be blank",
      });
  });
const reasoningEffort = z.enum([
  "none",
  "minimal",
  "low",
  "medium",
  "high",
  "xhigh",
  "max",
]);
const metricPart = z.string().regex(/^[a-z][a-z0-9_]{0,62}$/);
const metricDescription = z
  .string()
  .trim()
  .min(1)
  .refine(
    (value) => !value.includes("\n") && !value.includes("\r"),
    "must be a single line",
  )
  .refine(
    (value) => new TextEncoder().encode(value).byteLength <= 512,
    "must be at most 512 UTF-8 bytes",
  );
const requiredMetricKey = z
  .record(metricPart, metricDescription)
  .refine(
    (value) => Object.keys(value).length === 1,
    "must define exactly one metric key",
  );
const requiredMetricNamespace = z
  .record(metricPart, z.array(requiredMetricKey).min(1))
  .refine(
    (value) => Object.keys(value).length === 1,
    "must define exactly one metric namespace",
  );
const requiredMetric = z
  .object({
    namespace: metricPart,
    key: metricPart,
    description: metricDescription,
  })
  .strict();
const requiredMetrics = z
  .array(requiredMetricNamespace)
  .max(100)
  .transform((groups, ctx) => {
    const metrics = groups.flatMap((group) =>
      Object.entries(group).flatMap(([namespace, entries]) =>
        entries.flatMap((entry) =>
          Object.entries(entry).map(([key, description]) => ({
            namespace,
            key,
            description,
          })),
        ),
      ),
    );
    if (metrics.length > 100) {
      ctx.addIssue({
        code: "too_big",
        origin: "array",
        maximum: 100,
        inclusive: true,
        message: "must define at most 100 metrics",
      });
      return z.NEVER;
    }
    const names = new Set<string>();
    for (const metric of metrics) {
      const name = `${metric.namespace}.${metric.key}`;
      if (names.has(name)) {
        ctx.addIssue({
          code: "custom",
          message: `contains duplicate metric ${name}`,
        });
        return z.NEVER;
      }
      names.add(name);
    }
    return z.array(requiredMetric).parse(metrics);
  });
const readableTokenLimit = z.preprocess(
  (value) =>
    typeof value === "string" && /^[1-9]\d{0,2}(?:_\d{3})+$/.test(value)
      ? Number(value.replaceAll("_", ""))
      : value,
  z.number().int().min(1).max(Number.MAX_SAFE_INTEGER),
);
const tokenBudgetSchema = z
  .object({
    limit: readableTokenLimit,
    period: z.enum(["day", "week"]),
  })
  .strict();
const agentWorkflowStepSchema = z
  .object({
    id: name,
    prompt: promptPath,
    allow_user_input: z.boolean().default(false),
    harness: name.optional(),
    provider: name.optional(),
    model: modelName.optional(),
    reasoning_effort: reasoningEffort.nullable().optional(),
    required_metrics: requiredMetrics.default([]),
    timeout: executionDuration.optional(),
  })
  .strict();
const commandWorkflowStepSchema = z
  .object({
    id: name,
    command,
    timeout: executionDuration.optional(),
  })
  .strict();
const workflowScheduleSchema = z
  .object({
    id: name,
    cron: z.string().superRefine((value, ctx) => {
      if (!isCloudflareCronExpression(value)) {
        ctx.addIssue({
          code: "custom",
          message:
            "must be a canonical five-field Cloudflare Cron expression in UTC",
        });
      }
    }),
  })
  .strict();

export const workflowSchema = z
  .object({
    version: z.literal(1),
    name,
    memory: z.boolean().default(false),
    workflow_timeout: executionDuration,
    token_budget: tokenBudgetSchema.optional(),
    schedules: z.array(workflowScheduleSchema).max(250).default([]),
    defaults: z
      .object({
        step_timeout: executionDuration,
        harness: name.optional(),
        provider: name.optional(),
        model: modelName.optional(),
        reasoning_effort: reasoningEffort.optional(),
      })
      .strict(),
    steps: z
      .array(z.union([agentWorkflowStepSchema, commandWorkflowStepSchema]))
      .min(1)
      .max(100),
  })
  .strict()
  .superRefine((value, ctx) => {
    const seen = new Set<string>();
    value.steps.forEach((step, index) => {
      if (seen.has(step.id))
        ctx.addIssue({
          code: "custom",
          path: ["steps", index, "id"],
          message: "must be unique",
        });
      seen.add(step.id);
      if ("prompt" in step) {
        for (const field of ["harness", "provider", "model"] as const) {
          if (!step[field] && !value.defaults[field]) {
            ctx.addIssue({
              code: "custom",
              path: ["steps", index, field],
              message: `must be set on the step or workflow defaults`,
            });
          }
        }
      }
    });
    if (
      durationToMilliseconds(value.defaults.step_timeout) >
      durationToMilliseconds(value.workflow_timeout)
    ) {
      ctx.addIssue({
        code: "custom",
        path: ["defaults", "step_timeout"],
        message: "must not exceed workflow_timeout",
      });
    }
    const scheduleIds = new Set<string>();
    const scheduleCrons = new Set<string>();
    value.schedules.forEach((schedule, index) => {
      if (scheduleIds.has(schedule.id))
        ctx.addIssue({
          code: "custom",
          path: ["schedules", index, "id"],
          message: "must be unique",
        });
      if (scheduleCrons.has(schedule.cron))
        ctx.addIssue({
          code: "custom",
          path: ["schedules", index, "cron"],
          message:
            "must be unique because Cloudflare identifies triggers by expression",
        });
      scheduleIds.add(schedule.id);
      scheduleCrons.add(schedule.cron);
    });
  });

export type RunnerSettings = z.infer<typeof runnerSettingsSchema>;
export type Catalog = z.infer<typeof catalogSchema>;
export type ReasoningEffort = z.infer<typeof reasoningEffort>;
export type RequiredMetric = z.infer<typeof requiredMetric>;
export type Workflow = z.infer<typeof workflowSchema>;
export type WorkflowSchedule = z.infer<typeof workflowScheduleSchema>;
export type RunnerConfig = RunnerSettings &
  Omit<Catalog, "version"> & { catalogVersion: 1 };
export type WorkflowRunnerConfig = RunnerConfig & { workflow: Workflow };
export type ConfigurationEnvironment = Record<string, string | undefined>;

export interface DeploymentTarget {
  profile: EnvironmentProfile | null;
  deploymentName: string;
  storageBucketName: string;
}

const supportedProtocols: Record<
  z.infer<typeof harnessSchema>["type"],
  ReadonlySet<z.infer<typeof providerProtocol>>
> = {
  codex: new Set(["openai-responses"]),
  grok: new Set(["xai"]),
  "claude-code": new Set(["anthropic"]),
  opencode: new Set(["openai-responses", "anthropic", "openai-compatible"]),
  pi: new Set(["xai", "openai-responses", "anthropic", "openai-compatible"]),
};
const credentialHeaderNames = new Set([
  "authorization",
  "proxy-authorization",
  "cookie",
  "x-api-key",
  "api-key",
  "cf-aig-authorization",
]);
const reservedCredentialEnvironmentNames = new Set([
  "HOME",
  "CODEX_HOME",
  "GROK_HOME",
  "CLAUDE_CONFIG_DIR",
  "OPENCODE_HOME",
  "PI_CODING_AGENT_DIR",
  "CODEX_API_KEY",
  "OPENAI_API_KEY",
  "XAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "ANTHROPIC_AUTH_TOKEN",
  "CODEX_PROXY_TOKEN",
  "RUNNER_EGRESS_TOKEN",
  "RUNNER_PROVIDER_TOKEN",
  "RUNNER_CONFIG",
  "RUNNER_CONFIG_JSON",
  "OPENCODE_CONFIG_CONTENT",
  "OPENCODE_AUTH_CONTENT",
  "SSL_CERT_FILE",
  "NODE_EXTRA_CA_CERTS",
  "NODE_USE_ENV_PROXY",
  "GROK_CONFIG",
  "MODEL_PROVIDER",
  "ANTHROPIC_BASE_URL",
  "ANTHROPIC_CUSTOM_HEADERS",
  "ANTHROPIC_MODEL",
  "ANTHROPIC_CUSTOM_MODEL_OPTION",
  "ANTHROPIC_CUSTOM_MODEL_OPTION_SUPPORTED_CAPABILITIES",
  "CLAUDE_MODEL_CONFIG",
  "CLAUDE_CODE_EXECUTABLE",
  "CLAUDE_CODE_ALLOW_MODEL_CAPABILITY_OVERRIDES",
  "CLAUDE_CODE_EFFORT_LEVEL",
  "INITIAL_AGENT_MODE",
  "NO_BROWSER",
  "PI_ACP_PATH",
  "PI_OFFLINE",
  "PI_SKIP_VERSION_CHECK",
  "PI_TELEMETRY",
  "RUNNER_JOB_ID",
  "AGENT_MEMORY_DB",
]);
const reservedRuntimeEnvironmentNames = new Set([
  ...reservedCredentialEnvironmentNames,
  "PATH",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "TZ",
  "TMPDIR",
  "TMP",
  "TEMP",
  "HTTP_PROXY",
  "HTTPS_PROXY",
  "NO_PROXY",
  "SSL_CERT_DIR",
  "http_proxy",
  "https_proxy",
  "no_proxy",
]);

/** Converts a compact duration string into milliseconds. */
export function durationToMilliseconds(input: string): number {
  const match = /^(\d+)(ms|s|m|h|d)$/.exec(input);
  if (!match) throw new Error(`Invalid duration: ${input}`);
  const multipliers: Record<string, number> = {
    ms: 1,
    s: 1000,
    m: 60_000,
    h: 3_600_000,
    d: 86_400_000,
  };
  return Number(match[1]) * multipliers[match[2]!]!;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

async function readYamlFile(file: string): Promise<unknown> {
  return parseYaml(await readFile(file, "utf8"), { uniqueKeys: true });
}

async function readOptionalYamlFile(
  file: string,
): Promise<unknown | undefined> {
  try {
    return await readYamlFile(file);
  } catch (error) {
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ENOENT"
    )
      return undefined;
    throw error;
  }
}

/** Loads, merges, and cross-validates deployment policy and the execution catalog. */
export async function loadRunnerConfig(
  root = process.cwd(),
): Promise<RunnerConfig> {
  const configRoot = path.join(root, "config");
  const [runnerValue, catalogValue] = await Promise.all([
    readYamlFile(path.join(configRoot, "runner.yaml")),
    readYamlFile(path.join(configRoot, "catalog.yaml")),
  ]);
  const runner = runnerSettingsSchema.parse(runnerValue);
  const catalog = catalogSchema.parse(catalogValue);
  validateConfiguration(runner, catalog);
  const { version: catalogVersion, ...catalogValues } = catalog;
  return { ...runner, ...catalogValues, catalogVersion };
}

/** Loads and validates the deployment-local active workflow. */
export async function loadWorkflowConfig(
  root = process.cwd(),
): Promise<WorkflowRunnerConfig> {
  const config = await loadRunnerConfig(root);
  const workflow = await readOptionalWorkflow(
    config,
    path.join(root, "config", "workflow.yaml"),
  );
  if (!workflow) {
    throw new Error(
      "Missing config/workflow.yaml; copy config/workflow.yaml.example and customize it",
    );
  }
  return { ...config, workflow };
}

/** Loads an active workflow when present without making it mandatory for code generation. */
export async function loadOptionalWorkflow(
  config: RunnerConfig,
  root = process.cwd(),
): Promise<Workflow | undefined> {
  return readOptionalWorkflow(
    config,
    path.join(root, "config", "workflow.yaml"),
  );
}

/** Validates the committed workflow example and an active workflow when one exists. */
export async function validateWorkflowFiles(
  config: RunnerConfig,
  root = process.cwd(),
): Promise<boolean> {
  const configRoot = path.join(root, "config");
  return Boolean(await loadOptionalWorkflow(config, root));
}

async function readOptionalWorkflow(
  config: RunnerConfig,
  file: string,
): Promise<Workflow | undefined> {
  const value = await readOptionalYamlFile(file);
  if (value === undefined) return undefined;
  if (isRecord(value)) {
  }
  const workflow = workflowSchema.parse(value);
  validateWorkflow(config, workflow);
  return workflow;
}

/** Lists every Worker Secret consumed by one credential definition. */
function credentialSecretNames(
  credential: Catalog["credentials"][string],
): string[] {
  if ("secret" in credential.upstream) return [credential.upstream.secret];
  const oauth = credential.upstream.oauth;
  return [
    oauth.client_id_secret,
    ...(oauth.client_auth.method === "none" ? [] : [oauth.client_auth.secret]),
    ...(oauth.grant.type === "refresh_token"
      ? [oauth.grant.refresh_token_secret]
      : []),
  ];
}

/** Validates security invariants that span deployment policy and the catalog. */
function validateConfiguration(runner: RunnerSettings, catalog: Catalog): void {
  const errors: string[] = [];
  const secretNames = new Set([
    runner.api.auth_secret,

    ...Object.values(catalog.credentials).flatMap(credentialSecretNames),
  ]);
  for (const environmentName of Object.keys(catalog.environment)) {
    if (reservedRuntimeEnvironmentNames.has(environmentName))
      errors.push(
        `environment.${environmentName} conflicts with a reserved runner variable`,
      );
    if (secretNames.has(environmentName))
      errors.push(
        `environment.${environmentName} is also declared as a Worker Secret`,
      );
  }
  const environmentNames = new Set<string>();
  for (const [credentialName, credential] of Object.entries(
    catalog.credentials,
  )) {
    const credentialSecrets = credentialSecretNames(credential);
    if (credentialSecrets.includes(runner.api.auth_secret))
      errors.push(
        `credentials.${credentialName} must not reuse the public API bearer secret`,
      );
    const environmentName = credential.source.env;
    if (
      environmentName &&
      reservedCredentialEnvironmentNames.has(environmentName)
    )
      errors.push(
        `credentials.${credentialName}.source.env conflicts with a reserved runner variable`,
      );
    if (environmentName && environmentName in catalog.environment)
      errors.push(
        `credentials.${credentialName}.source.env conflicts with a plaintext environment variable`,
      );
    if (environmentName && environmentNames.has(environmentName))
      errors.push(`credentials.${credentialName}.source.env must be unique`);
    if (environmentName) environmentNames.add(environmentName);
  }
  for (const [providerName, provider] of Object.entries(catalog.providers)) {
    if (!(provider.credential in catalog.credentials))
      errors.push(
        `providers.${providerName}.credential references an unknown credential`,
      );
    if (
      provider.endpoint.kind === "cloudflare-ai" &&
      provider.protocol === "xai"
    )
      errors.push(
        `providers.${providerName}.protocol cannot use xai with cloudflare-ai`,
      );
    for (const header of Object.keys(provider.static_headers)) {
      if (credentialHeaderNames.has(header.toLowerCase()))
        errors.push(
          `providers.${providerName}.static_headers.${header} must use credential injection`,
        );
    }
  }
  for (const [routeName, route] of Object.entries(catalog.routes)) {
    if (!(route.credential in catalog.credentials))
      errors.push(
        `routes.${routeName}.credential references an unknown credential`,
      );
  }
  for (const [modelName, model] of Object.entries(catalog.models)) {
    if (Object.keys(model.providers).length === 0)
      errors.push(`models.${modelName}.providers must not be empty`);
    for (const [providerName, providerModel] of Object.entries(
      model.providers,
    )) {
      const provider = catalog.providers[providerName];
      if (!provider)
        errors.push(
          `models.${modelName}.providers.${providerName} references an unknown provider`,
        );
      else if (
        provider.protocol === "openai-compatible" &&
        /^(?:openai\/|gpt-|o\d)/i.test(providerModel)
      )
        errors.push(
          `models.${modelName}.providers.${providerName}: OpenAI-family models must use openai-responses`,
        );
    }
  }
  if (errors.length > 0) throw new Error(errors.join("\n"));
}

/** Validates workflow references against the effective deployment configuration. */
function validateWorkflow(config: RunnerConfig, workflow: Workflow): void {
  const errors: string[] = [];
  if (workflow.memory && config.container.max_instances !== 1) {
    errors.push("workflow.memory requires container.max_instances to equal 1");
  }
  const workflowTimeout = durationToMilliseconds(workflow.workflow_timeout);
  const defaults = workflow.defaults;
  const validateSelection = (
    prefix: string,
    harnessName: string,
    providerName: string,
    modelName: string,
  ): void => {
    const harness = config.harnesses[harnessName];
    const provider = config.providers[providerName];
    const model = config.models[modelName];
    if (!harness)
      errors.push(`${prefix}.harness references an unknown harness`);
    if (!provider)
      errors.push(`${prefix}.provider references an unknown provider`);
    if (!model) errors.push(`${prefix}.model references an unknown model`);
    if (model && !(providerName in model.providers))
      errors.push(
        `${prefix} model ${modelName} does not support provider ${providerName}`,
      );
    if (
      harness &&
      provider &&
      !supportedProtocols[harness.type].has(provider.protocol)
    )
      errors.push(
        `${prefix}: ${harness.type} does not support ${provider.protocol}`,
      );
  };
  const defaultHarness = defaults.harness
    ? config.harnesses[defaults.harness]
    : undefined;
  const defaultProvider = defaults.provider
    ? config.providers[defaults.provider]
    : undefined;
  const defaultModel = defaults.model
    ? config.models[defaults.model]
    : undefined;
  if (defaults.harness && !defaultHarness)
    errors.push("workflow.defaults.harness references an unknown harness");
  if (defaults.provider && !defaultProvider)
    errors.push("workflow.defaults.provider references an unknown provider");
  if (defaults.model && !defaultModel)
    errors.push("workflow.defaults.model references an unknown model");
  if (
    defaults.harness &&
    defaults.provider &&
    defaultHarness &&
    defaultProvider &&
    !supportedProtocols[defaultHarness.type].has(defaultProvider.protocol)
  ) {
    errors.push(
      `workflow.defaults: ${defaultHarness.type} does not support ${defaultProvider.protocol}`,
    );
  }
  if (
    defaults.model &&
    defaults.provider &&
    defaultModel &&
    !(defaults.provider in defaultModel.providers)
  ) {
    errors.push(
      `workflow default model ${defaults.model} does not support provider ${defaults.provider}`,
    );
  }
  for (const [index, step] of workflow.steps.entries()) {
    if ("command" in step) {
      if (
        step.timeout &&
        durationToMilliseconds(step.timeout) > workflowTimeout
      )
        errors.push(
          `workflow.steps.${index}.timeout exceeds workflow.workflow_timeout`,
        );
      continue;
    }
    const harnessName = step.harness ?? defaults.harness!;
    const harness = config.harnesses[harnessName];
    if (step.allow_user_input && config.interactions.provider === "none") {
      errors.push(
        `workflow.steps.${index}.allow_user_input requires an interaction provider`,
      );
    }
    if (
      step.allow_user_input &&
      (harness?.type === "pi" || harness?.type === "opencode")
    ) {
      errors.push(
        `workflow.steps.${index}.allow_user_input is unsupported for ${harness.type}`,
      );
    }
    validateSelection(
      `workflow.steps.${index}`,
      harnessName,
      step.provider ?? defaults.provider!,
      step.model ?? defaults.model!,
    );
    if (step.timeout && durationToMilliseconds(step.timeout) > workflowTimeout)
      errors.push(
        `workflow.steps.${index}.timeout exceeds workflow.workflow_timeout`,
      );
  }
  if (errors.length > 0) throw new Error(errors.join("\n"));
}

async function readEnvironmentFile(
  filename: string,
  required: boolean,
): Promise<Record<string, string>> {
  try {
    const metadata = await lstat(filename);
    if (!metadata.isFile() || metadata.isSymbolicLink())
      throw new Error(`${filename} must be a regular file`);
    if (metadata.size > 1024 * 1024)
      throw new Error(`${filename} exceeds 1 MiB`);
    return parseEnv(await readFile(filename, "utf8"));
  } catch (error) {
    if (
      !(
        error &&
        typeof error === "object" &&
        "code" in error &&
        error.code === "ENOENT"
      )
    )
      throw error;
    if (required)
      throw new Error(`Missing environment profile file: ${filename}`);
    return {};
  }
}

/** Loads the three local environment domains without allowing files to override one another. */
export async function loadConfigurationEnvironments(
  root = process.cwd(),
  profile?: EnvironmentProfile,
) {
  const files = await resolveEnvironmentFiles(root, profile);
  const profiled = files.profile !== null;
  const [runtimeLocal, secretsLocal, cloudflareLocal] = await Promise.all([
    readEnvironmentFile(files.runtime, false),
    readEnvironmentFile(files.secrets, profiled),
    readEnvironmentFile(files.cloudflare, profiled),
  ]);
  const owners = new Map<string, string>();
  const displayFile = (filename: string) =>
    path.relative(root, filename) || filename;
  for (const [filename, values] of [
    [files.runtime, runtimeLocal],
    [files.secrets, secretsLocal],
    [files.cloudflare, cloudflareLocal],
  ] as const) {
    const displayed = displayFile(filename);
    for (const name of Object.keys(values)) {
      const previous = owners.get(name);
      if (previous)
        throw new Error(
          `${name} is defined in both ${previous} and ${displayed}`,
        );
      owners.set(name, displayed);
    }
  }
  return {
    runtime: { ...runtimeLocal, ...process.env },
    secrets: { ...secretsLocal, ...process.env },
    cloudflare: { ...cloudflareLocal, ...process.env },
    local: {
      runtime: runtimeLocal,
      secrets: secretsLocal,
      cloudflare: cloudflareLocal,
    },
    files,
  } satisfies {
    runtime: ConfigurationEnvironment;
    secrets: ConfigurationEnvironment;
    cloudflare: ConfigurationEnvironment;
    local: Record<"runtime" | "secrets" | "cloudflare", Record<string, string>>;
    files: typeof files;
  };
}

/** Lists local runtime variables that are not declared by the effective catalog. */
export function undeclaredRuntimeEnvironmentNames(
  config: RunnerConfig,
  localEnvironment: ConfigurationEnvironment,
): string[] {
  return Object.keys(localEnvironment)
    .filter((name) => !(name in config.environment))
    .sort();
}

/** Produces an R2-safe bucket name solely from the Worker deployment name. */
export function storageBucketName(workerName: string): string {
  const direct = `${workerName}-storage`;
  if (direct.length <= 63) return direct;
  const digest = createHash("sha256")
    .update(workerName)
    .digest("hex")
    .slice(0, 8);
  return `${workerName.slice(0, 54)}-${digest}`;
}

/** Derives an environment-isolated Worker name while preserving the production identity. */
export function environmentDeploymentName(
  workerName: string,
  profile: EnvironmentProfile | null,
): string {
  if (profile !== "local") return workerName;
  const direct = `${workerName}-local`;
  if (direct.length <= 63) return direct;
  const digest = createHash("sha256").update(direct).digest("hex").slice(0, 8);
  return `${workerName.slice(0, 48)}-${digest}-local`;
}

/** Resolves the Worker and bucket identities owned by one environment profile. */
export function resolveDeploymentTarget(
  config: RunnerConfig,
  profile: EnvironmentProfile | null,
): DeploymentTarget {
  const deploymentName = environmentDeploymentName(
    config.deployment.name,
    profile,
  );
  return {
    profile,
    deploymentName,
    storageBucketName: storageBucketName(deploymentName),
  };
}

function resolveProviderBaseURL(
  provider: z.infer<typeof providerSchema>,
  environment: ConfigurationEnvironment,
): string {
  if (provider.endpoint.kind === "url")
    return provider.endpoint.base_url.replace(/\/$/, "");
  const accountId = environment.CLOUDFLARE_ACCOUNT_ID?.trim();
  if (!accountId)
    throw new Error(
      "Missing required environment variable: CLOUDFLARE_ACCOUNT_ID",
    );
  if (!accountIdPattern.test(accountId))
    throw new Error(
      "CLOUDFLARE_ACCOUNT_ID must be a 32-character hexadecimal account ID",
    );
  const root = `https://api.cloudflare.com/client/v4/accounts/${accountId}/ai`;
  return provider.protocol === "anthropic" ? root : `${root}/v1`;
}

/** Derives normalized values consumed by the Worker and container runtimes. */
export function toRuntimeConfig(
  config: RunnerConfig,
  environment: ConfigurationEnvironment = {},
  profile: EnvironmentProfile | null = null,
) {
  const deploymentTarget = resolveDeploymentTarget(config, profile);
  const shutdownGraceMs = durationToMilliseconds(config.runner.shutdown_grace);
  const retentionMs = durationToMilliseconds(config.api.retention);
  const providers = Object.fromEntries(
    Object.entries(config.providers).map(([providerName, provider]) => [
      providerName,
      {
        protocol: provider.protocol,
        base_url: resolveProviderBaseURL(provider, environment),
        static_headers: Object.fromEntries(
          Object.entries(provider.static_headers).map(([header, value]) => [
            header.toLowerCase(),
            value,
          ]),
        ),
      },
    ]),
  );
  const credentials = config.credentials;
  const oauthCredentials = Object.fromEntries(
    Object.entries(credentials).flatMap(([credentialName, credential]) => {
      if (!("oauth" in credential.upstream)) return [];
      const oauth = credential.upstream.oauth;
      return [
        [
          credentialName,
          {
            tokenUrl: oauth.token_url,
            clientIdSecret: oauth.client_id_secret,
            clientAuth:
              oauth.client_auth.method === "none"
                ? { method: oauth.client_auth.method }
                : {
                    method: oauth.client_auth.method,
                    secret: oauth.client_auth.secret,
                  },
            grant:
              oauth.grant.type === "client_credentials"
                ? { type: oauth.grant.type }
                : {
                    type: oauth.grant.type,
                    refreshTokenSecret: oauth.grant.refresh_token_secret,
                  },
            scopes: oauth.scopes,
            extraParameters: oauth.extra_parameters,
            fallbackTtlMs: oauth.fallback_ttl
              ? durationToMilliseconds(oauth.fallback_ttl)
              : null,
          },
        ],
      ];
    }),
  );
  const credentialInjections = [
    ...Object.entries(config.providers).map(([providerName, provider]) => ({
      targetKind: "provider" as const,
      targetName: providerName,
      urlPrefix: providers[providerName]!.base_url,
      credentialName: provider.credential,
      credential: credentials[provider.credential]!,
    })),
    ...Object.entries(config.routes).map(([routeName, route]) => ({
      targetKind: "route" as const,
      targetName: routeName,
      urlPrefix: route.url_prefix.replace(/\/$/, ""),
      credentialName: route.credential,
      credential: credentials[route.credential]!,
    })),
  ].map(
    ({ targetKind, targetName, urlPrefix, credentialName, credential }) => ({
      targetKind,
      targetName,
      urlPrefix,
      credentialName,
      sourceHeader: credential.source.header.toLowerCase(),
      sourceValuePrefix: credential.source.value_prefix,
      upstream:
        "oauth" in credential.upstream
          ? { kind: "oauth" as const }
          : {
              kind: "static" as const,
              header: credential.upstream.header.toLowerCase(),
              secret: credential.upstream.secret,
              valuePrefix: credential.upstream.value_prefix,
            },
    }),
  );
  const credentialEnvironment = [
    ...new Set(
      Object.values(credentials).flatMap((credential) =>
        credential.source.env ? [credential.source.env] : [],
      ),
    ),
  ];
  const runtimeEnvironment = Object.keys(config.environment).sort();
  const cloudflareAccountId = environment.CLOUDFLARE_ACCOUNT_ID?.trim();
  const configuredRunnerURL = environment.RUNNER_URL?.trim();
  let runnerUrl: string | null = null;
  if (configuredRunnerURL) {
    const parsed = new URL(configuredRunnerURL);
    const loopback =
      parsed.hostname === "localhost" ||
      parsed.hostname === "127.0.0.1" ||
      parsed.hostname === "[::1]";
    if (
      (parsed.protocol !== "https:" &&
        !(parsed.protocol === "http:" && loopback)) ||
      parsed.username ||
      parsed.password ||
      parsed.pathname !== "/" ||
      parsed.search ||
      parsed.hash
    ) {
      throw new Error(
        "RUNNER_URL must be an HTTPS origin, except for HTTP loopback, without credentials, a path, a query, or a fragment",
      );
    }
    runnerUrl = parsed.origin;
  }
  const checkpointLimits =
    config.interactions.provider === "none"
      ? {
          maxFiles: 10_000,
          maxFileBytes: 134_217_728,
          maxTotalBytes: 268_435_456,
        }
      : {
          maxFiles: config.interactions.checkpoint.max_files,
          maxFileBytes: config.interactions.checkpoint.max_file_bytes,
          maxTotalBytes: config.interactions.checkpoint.max_total_bytes,
        };
  const runnerConfig = {
    port: config.container.port,
    shutdown_grace_ms: shutdownGraceMs,
    max_prompt_bytes: config.runner.max_prompt_bytes,
    max_prompt_files: config.runner.max_prompt_files,
    max_prompt_bundle_bytes: config.runner.max_prompt_bundle_bytes,
    max_asset_bytes: config.runner.max_asset_bytes,
    max_asset_files: config.runner.max_asset_files,
    max_asset_bundle_bytes: config.runner.max_asset_bundle_bytes,
    max_result_bytes: config.runner.max_result_bytes,
    max_artifact_files: config.artifacts.max_files,
    max_artifact_file_bytes: config.artifacts.max_file_bytes,
    max_artifact_total_bytes: config.artifacts.max_total_bytes,
    max_memory_bytes: config.memory.max_database_bytes,
    memory_persistence_timeout_ms: durationToMilliseconds(
      config.memory.persistence_timeout,
    ),
    max_log_bytes: config.logging.max_log_bytes,
    providers,
    models: Object.fromEntries(
      Object.entries(config.models).map(([modelName, model]) => [
        modelName,
        model.providers,
      ]),
    ),
    harnesses: config.harnesses,
    interactions:
      config.interactions.provider === "none"
        ? {
            provider: "none" as const,
            checkpoint_max_files: checkpointLimits.maxFiles,
            checkpoint_max_file_bytes: checkpointLimits.maxFileBytes,
            checkpoint_max_total_bytes: checkpointLimits.maxTotalBytes,
          }
        : {
            provider: "callback" as const,
            live_wait_timeout_ms: durationToMilliseconds(
              config.interactions.live_wait,
            ),
            max_request_bytes: config.interactions.max_request_bytes,
            max_response_bytes: config.interactions.max_response_bytes,
            checkpoint_max_files: config.interactions.checkpoint.max_files,
            checkpoint_max_file_bytes:
              config.interactions.checkpoint.max_file_bytes,
            checkpoint_max_total_bytes:
              config.interactions.checkpoint.max_total_bytes,
          },
    credential_environment: credentialEnvironment,
    runtime_environment: runtimeEnvironment,
  } as const;
  return {
    version: config.version,
    deploymentName: deploymentTarget.deploymentName,
    storageBucketName: deploymentTarget.storageBucketName,
    cloudflareAccountId: cloudflareAccountId ?? null,
    containerPort: config.container.port,
    maxInstances: config.container.max_instances,
    authSecretName: config.api.auth_secret,
    retentionMs,
    shutdownGraceMs,
    maxPromptBytes: config.runner.max_prompt_bytes,
    maxPromptFiles: config.runner.max_prompt_files,
    maxPromptBundleBytes: config.runner.max_prompt_bundle_bytes,
    maxAssetBytes: config.runner.max_asset_bytes,
    maxAssetFiles: config.runner.max_asset_files,
    maxAssetBundleBytes: config.runner.max_asset_bundle_bytes,
    maxResultBytes: config.runner.max_result_bytes,
    artifactLimits: {
      maxFiles: config.artifacts.max_files,
      maxFileBytes: config.artifacts.max_file_bytes,
      maxTotalBytes: config.artifacts.max_total_bytes,
    },
    memoryLimits: {
      maxDatabaseBytes: config.memory.max_database_bytes,
      persistenceTimeoutMs: durationToMilliseconds(
        config.memory.persistence_timeout,
      ),
    },
    checkpointLimits,
    interactions:
      config.interactions.provider === "none"
        ? { provider: "none" as const }
        : {
            provider: "slack" as const,
            liveWaitMs: durationToMilliseconds(config.interactions.live_wait),
            responseTtlMs: durationToMilliseconds(
              config.interactions.response_ttl,
            ),
            maxRequestBytes: config.interactions.max_request_bytes,
            maxResponseBytes: config.interactions.max_response_bytes,
            checkpointLimits: {
              maxFiles: config.interactions.checkpoint.max_files,
              maxFileBytes: config.interactions.checkpoint.max_file_bytes,
              maxTotalBytes: config.interactions.checkpoint.max_total_bytes,
            },
            teamId: config.interactions.team_id,
            conversationId: config.interactions.conversation_id,
            allowedUserIds: config.interactions.allowed_user_ids,
            botTokenSecret: config.interactions.bot_token_secret,
            signingSecret: config.interactions.signing_secret,
          },
    historyLimits: {
      maxVerboseBytes: config.history.max_verbose_bytes,
      maxMetricBytes: config.history.max_metric_bytes,
      maxLifecycleBytes: config.history.max_lifecycle_bytes,
      maxPendingBytes: config.history.max_pending_bytes,
    },
    runnerUrl,
    providers,
    models: config.models,
    harnesses: config.harnesses,
    runtimeEnvironment,
    credentialInjections,
    oauthCredentials,
    oauthPolicy: {
      tokenRequestTimeoutMs: durationToMilliseconds(
        config.egress.oauth.token_request_timeout,
      ),
      expirySkewMs: durationToMilliseconds(config.egress.oauth.expiry_skew),
      maxTokenRequestBytes: config.egress.oauth.max_token_request_bytes,
      maxTokenResponseBytes: config.egress.oauth.max_token_response_bytes,
      maxTokenBytes: config.egress.oauth.max_token_bytes,
    },
    logMode: config.logging.events,
    maxLogBytes: config.logging.max_log_bytes,
    runnerConfig,
  } as const;
}

/** Resolves catalog-declared plaintext bindings without exposing undeclared local values. */
export function resolveRuntimeEnvironment(
  config: RunnerConfig,
  environment: ConfigurationEnvironment,
): Record<string, string> {
  const values: Record<string, string> = {};
  const missing: string[] = [];
  for (const name of Object.keys(config.environment).sort()) {
    const value = environment[name];
    if (!value?.trim()) missing.push(name);
    else values[name] = value;
  }
  if (missing.length > 0)
    throw new Error(
      `Missing required plaintext environment variables in .env: ${missing.join(", ")}`,
    );
  return values;
}

/** Returns all Worker Secret names referenced by the effective configuration. */
export function requiredSecretNames(
  config: RunnerConfig,
  workflow?: Workflow,
): string[] {
  const interactionSecrets =
    config.interactions.provider === "slack"
      ? [
          config.interactions.bot_token_secret,
          config.interactions.signing_secret,
        ]
      : [];
  const active =
    workflow ??
    ("workflow" in config
      ? (config as WorkflowRunnerConfig).workflow
      : undefined);
  const selected = new Set(
    Object.values(config.routes).map((route) => route.credential),
  );
  if (active)
    for (const step of active.steps) {
      if ("command" in step) continue;
      const provider = step.provider ?? active.defaults.provider;
      if (provider && config.providers[provider])
        selected.add(config.providers[provider]!.credential);
    }
  return [
    ...new Set([
      config.api.auth_secret,
      ...interactionSecrets,
      ...[...selected].flatMap((name) =>
        credentialSecretNames(config.credentials[name]!),
      ),
    ]),
  ].sort();
}
