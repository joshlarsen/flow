import { appConfig } from "./generated-config.ts";
import {
  activeWorkflowKey,
  workflowBundleArchiveKey,
  workflowBundleManifestKey,
} from "./storage-keys.ts";
import type { Env, WorkflowBundle, WorkflowBundleManifest, WorkflowBundleFileKind } from "./types.ts";

const namePattern = /^[a-z][a-z0-9_-]{0,63}$/;
const digestPattern = /^[0-9a-f]{64}$/;
const sortKeyPattern = /^\d{8}T\d{6}\.\d{3}Z$/;
const promptPathPattern = /^[A-Za-z0-9][A-Za-z0-9._/-]*\.md$/;
const maxManifestBytes = 2 * 1024 * 1024;
const maxCommandArguments = 64;
const maxCommandArgumentBytes = 4096;
const reasoningEfforts = new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max"]);
const metricPartPattern = /^[a-z][a-z0-9_]{0,62}$/;
const encoder = new TextEncoder();
const supportedProtocols: Record<string, ReadonlySet<string>> = {
  codex: new Set(["openai-responses"]), grok: new Set(["xai"]), "claude-code": new Set(["anthropic"]),
  opencode: new Set(["openai-responses", "anthropic", "openai-compatible"]),
  pi: new Set(["xai", "openai-responses", "anthropic", "openai-compatible"]),
};

function safeBundlePath(value: string): boolean {
  if (!value || value.startsWith("/") || value.includes("\\") || encoder.encode(value).byteLength > 1024) return false;
  return value.split("/").every((part) => part !== "" && part !== "." && part !== ".." && !part.includes("\0") && encoder.encode(part).byteLength <= 255);
}

function safePromptPath(value: string): boolean {
  return promptPathPattern.test(value) && safeBundlePath(value) && value.split("/").every((part) => !part.startsWith("."));
}

/** Validates one normalized required-metric definition from a workflow bundle. */
function isRequiredMetric(value: unknown): value is { namespace: string; key: string; description: string } {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const metric = value as Record<string, unknown>;
  return Object.keys(metric).length === 3 && Object.keys(metric).every((key) => ["namespace", "key", "description"].includes(key)) &&
    typeof metric.namespace === "string" && metricPartPattern.test(metric.namespace) &&
    typeof metric.key === "string" && metricPartPattern.test(metric.key) &&
    typeof metric.description === "string" && metric.description === metric.description.trim() && metric.description.length > 0 && encoder.encode(metric.description).byteLength <= 512 &&
    !metric.description.includes("\n") && !metric.description.includes("\r");
}

async function digest(value: ArrayBuffer): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", value);
  return [...new Uint8Array(hash)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/** Validates an active manifest against the deployed workflow catalog and resource limits. */
export function isWorkflowBundleManifest(value: unknown): value is WorkflowBundleManifest {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const item = value as Record<string, unknown>;
  if (item.version !== 1 || typeof item.digest !== "string" || !digestPattern.test(item.digest) || typeof item.sort_key !== "string" || !sortKeyPattern.test(item.sort_key)) return false;
  if (!item.workflow || typeof item.workflow !== "object" || Array.isArray(item.workflow)) return false;
  const workflow = item.workflow as Record<string, unknown>;
  if (Object.keys(workflow).some((key) => !["version", "name", "memory_enabled", "workflow_timeout_ms", "token_budget", "default_step_timeout_ms", "default_harness", "default_model", "default_reasoning_effort", "steps"].includes(key)) ||
    workflow.version !== 1 || typeof workflow.name !== "string" || !namePattern.test(workflow.name) ||
    !(workflow.memory_enabled === undefined || typeof workflow.memory_enabled === "boolean") ||
    (workflow.memory_enabled === true && appConfig.maxInstances !== 1) ||
    typeof workflow.workflow_timeout_ms !== "number" || !Number.isSafeInteger(workflow.workflow_timeout_ms) || workflow.workflow_timeout_ms < 1 ||
    typeof workflow.default_step_timeout_ms !== "number" || !Number.isSafeInteger(workflow.default_step_timeout_ms) || workflow.default_step_timeout_ms < 1 ||
    workflow.default_step_timeout_ms > workflow.workflow_timeout_ms ||
    !(workflow.token_budget === undefined || isTokenBudget(workflow.token_budget)) ||
    !(workflow.default_harness === undefined || workflow.default_harness === null || typeof workflow.default_harness === "string" && Object.hasOwn(appConfig.harnesses, workflow.default_harness)) ||
    !(workflow.default_model === undefined || workflow.default_model === null || typeof workflow.default_model === "string" && Object.hasOwn(appConfig.models, workflow.default_model)) ||
    !(workflow.default_reasoning_effort === undefined || workflow.default_reasoning_effort === null || typeof workflow.default_reasoning_effort === "string" && reasoningEfforts.has(workflow.default_reasoning_effort)) ||
    !Array.isArray(workflow.steps) || workflow.steps.length === 0 || workflow.steps.length > 100) return false;
  const workflowTimeoutMs = workflow.workflow_timeout_ms;
  const stepIds = new Set<string>();
  for (const raw of workflow.steps) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return false;
    const step = raw as Record<string, unknown>;
    if (typeof step.id !== "string" || !namePattern.test(step.id) || stepIds.has(step.id) || typeof step.timeout_ms !== "number" || !Number.isSafeInteger(step.timeout_ms) || step.timeout_ms < 1 || step.timeout_ms > workflowTimeoutMs) return false;
    if (Array.isArray(step.command)) {
      if (Object.keys(step).some((key) => !["id", "command", "timeout_ms"].includes(key)) || step.command.length < 1 || step.command.length > maxCommandArguments ||
        step.command.some((argument) => typeof argument !== "string" || encoder.encode(argument).byteLength > maxCommandArgumentBytes) || typeof step.command[0] !== "string" || step.command[0].trim().length === 0) return false;
      stepIds.add(step.id);
      continue;
    }
    if (Object.keys(step).some((key) => !["id", "prompt", "allow_user_input", "harness", "provider", "model", "reasoning_effort", "required_metrics", "timeout_ms"].includes(key)) || typeof step.prompt !== "string" || !safePromptPath(step.prompt) ||
      !(step.allow_user_input === undefined || typeof step.allow_user_input === "boolean") || (step.allow_user_input === true && appConfig.interactions.provider === "none") ||
      !Array.isArray(step.required_metrics) || step.required_metrics.length > 100 || step.required_metrics.some((metric) => !isRequiredMetric(metric)) ||
      new Set(step.required_metrics.map((metric) => `${(metric as Record<string, unknown>).namespace}.${(metric as Record<string, unknown>).key}`)).size !== step.required_metrics.length ||
      typeof step.harness !== "string" || !Object.hasOwn(appConfig.harnesses, step.harness) || typeof step.provider !== "string" || !Object.hasOwn(appConfig.providers, step.provider) ||
      typeof step.model !== "string" || !Object.hasOwn(appConfig.models, step.model) ||
      !(step.reasoning_effort === null || typeof step.reasoning_effort === "string" && reasoningEfforts.has(step.reasoning_effort))) return false;
    const model = appConfig.models[step.model as keyof typeof appConfig.models];
    if (!model || !Object.hasOwn(model.providers, step.provider)) return false;
    const harness = appConfig.harnesses[step.harness as keyof typeof appConfig.harnesses];
    const provider = appConfig.providers[step.provider as keyof typeof appConfig.providers];
    if (!harness || !provider || !supportedProtocols[harness.type]?.has(provider.protocol)) return false;
    if (step.allow_user_input === true && (harness.type === "pi" || harness.type === "opencode")) return false;
    stepIds.add(step.id);
  }
  if (!item.archive || typeof item.archive !== "object" || Array.isArray(item.archive)) return false;
  const archive = item.archive as Record<string, unknown>;
  const archiveLimit = appConfig.maxPromptBundleBytes + appConfig.maxAssetBundleBytes + (appConfig.maxPromptFiles + appConfig.maxAssetFiles + 16) * 1024 + 1024 * 1024;
  if (Object.keys(archive).some((key) => !["key", "size", "sha256"].includes(key)) || typeof archive.sha256 !== "string" || !digestPattern.test(archive.sha256) ||
    typeof archive.size !== "number" || !Number.isSafeInteger(archive.size) || archive.size < 1 || archive.size > archiveLimit ||
    archive.key !== workflowBundleArchiveKey(item.sort_key, item.digest)) return false;
  if (!Array.isArray(item.files) || typeof item.total_bytes !== "number" || !Number.isSafeInteger(item.total_bytes) || item.total_bytes < 0) return false;
  let promptCount = 0; let assetCount = 0; let promptBytes = 0; let assetBytes = 0;
  const paths = new Set<string>();
  const promptPaths = new Set<string>();
  for (const raw of item.files) {
    if (!raw || typeof raw !== "object" || Array.isArray(raw)) return false;
    const file = raw as Record<string, unknown>;
    if (!(["prompt", "script", "skill"] as WorkflowBundleFileKind[]).includes(file.kind as WorkflowBundleFileKind) || typeof file.path !== "string" || !safeBundlePath(file.path) ||
      typeof file.size !== "number" || !Number.isSafeInteger(file.size) || file.size < 0 || typeof file.sha256 !== "string" || !digestPattern.test(file.sha256) || typeof file.executable !== "boolean") return false;
    const identity = `${file.kind}:${file.path}`;
    if (paths.has(identity)) return false;
    paths.add(identity);
    if (file.kind === "prompt") {
      if (!safePromptPath(file.path) || file.size < 1 || file.size > appConfig.maxPromptBytes) return false;
      promptCount++; promptBytes += file.size; promptPaths.add(file.path);
    } else {
      if (file.size > appConfig.maxAssetBytes) return false;
      assetCount++; assetBytes += file.size;
    }
  }
  if (promptCount > appConfig.maxPromptFiles || promptBytes > appConfig.maxPromptBundleBytes || assetCount > appConfig.maxAssetFiles || assetBytes > appConfig.maxAssetBundleBytes || promptBytes + assetBytes !== item.total_bytes) return false;
  return workflow.steps.every((step) => Array.isArray((step as Record<string, unknown>).command) || promptPaths.has((step as { prompt: string }).prompt));
}

function isTokenBudget(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const budget = value as Record<string, unknown>;
  return Object.keys(budget).every((key) => key === "limit" || key === "period") &&
    typeof budget.limit === "number" && Number.isSafeInteger(budget.limit) && budget.limit > 0 &&
    (budget.period === "day" || budget.period === "week");
}

/** Loads the active readable manifest without fetching its archive. */
export async function loadActiveWorkflowBundle(env: Env): Promise<WorkflowBundle> {
  const object = await env.RUNNER_STORAGE.get(activeWorkflowKey);
  if (!object) throw new Error("No active workflow bundle is configured");
  if (object.size > maxManifestBytes) throw new Error("Active workflow manifest is too large");
  const manifestValue: unknown = JSON.parse(await object.text());
  if (!isWorkflowBundleManifest(manifestValue)) throw new Error("Active workflow manifest is invalid or incompatible with this deployment");
  return { manifest: manifestValue };
}

/** Returns the immutable readable-manifest key associated with a validated bundle. */
export function workflowManifestKey(manifest: WorkflowBundleManifest): string {
  return workflowBundleManifestKey(manifest.sort_key, manifest.digest);
}

/** Summarizes a materialized bundle without copying file paths or contents into telemetry. */
export function workflowBundleTelemetry(manifest: WorkflowBundleManifest): object {
  const byKind = {
    prompt: { count: 0, bytes: 0 },
    script: { count: 0, bytes: 0 },
    skill: { count: 0, bytes: 0 },
  };
  for (const file of manifest.files) {
    byKind[file.kind].count++;
    byKind[file.kind].bytes += file.size;
  }
  return {
    manifest_version: manifest.version,
    digest: manifest.digest,
    sort_key: manifest.sort_key,
    archive: manifest.archive,
    files: {
      count: manifest.files.length,
      total_bytes: manifest.total_bytes,
      by_kind: byKind,
    },
  };
}

/** Loads a pinned readable manifest and verifies both its location and expected digest. */
export async function loadPinnedWorkflowBundle(env: Env, key: string, expectedDigest: string): Promise<WorkflowBundle> {
  const prefix = "bundles/";
  if (!key.startsWith(prefix) || !key.endsWith("/manifest.json") || key.includes("..")) throw new Error("Pinned workflow manifest key is invalid");
  const object = await env.RUNNER_STORAGE.get(key);
  if (!object) throw new Error("Pinned workflow manifest is missing");
  if (object.size > maxManifestBytes) throw new Error("Pinned workflow manifest is too large");
  const manifestValue: unknown = JSON.parse(await object.text());
  if (!isWorkflowBundleManifest(manifestValue) || manifestValue.digest !== expectedDigest || workflowManifestKey(manifestValue) !== key) {
    throw new Error("Pinned workflow manifest is invalid or does not match the scheduled occurrence");
  }
  return { manifest: manifestValue };
}

/** Fetches and integrity-checks the single archive pinned by a validated manifest. */
export async function loadWorkflowBundleArchive(env: Env, manifest: WorkflowBundleManifest): Promise<Uint8Array> {
  const object = await env.RUNNER_STORAGE.get(manifest.archive.key);
  if (!object) throw new Error("Workflow bundle archive is missing");
  if (object.size !== manifest.archive.size) throw new Error("Workflow bundle archive has an invalid size");
  const bytes = await object.arrayBuffer();
  if (bytes.byteLength !== manifest.archive.size || await digest(bytes) !== manifest.archive.sha256) throw new Error("Workflow bundle archive failed integrity validation");
  return new Uint8Array(bytes);
}
