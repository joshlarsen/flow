#!/usr/bin/env node

import path from "node:path";
import { fileURLToPath } from "node:url";
import { durationToMilliseconds, loadConfigurationEnvironments, loadWorkflowConfig, requiredSecretNames } from "../tools/config.ts";
import { buildWorkflowBundle } from "../tools/workflow-bundle.ts";
import { FlowClient } from "../tools/flow-client.ts";
import { requireRunnerPreflight } from "./runner-preflight.ts";

const terminalStatuses = new Set(["succeeded", "partial", "failed", "timed_out", "cancelled", "interrupted"]);
const localRunnerURL = "http://127.0.0.1:8787";
const eventDeliveryTimeoutMs = 120_000;

export function requireEnvironment(names: string[], environment: Record<string, string | undefined>): Record<string, string> {
  const missing = [...new Set(names)].filter((name) => !environment[name]?.trim());
  if (missing.length > 0) throw new Error(`Missing required environment variables: ${missing.join(", ")}`);
  return Object.fromEntries(names.map((name) => [name, environment[name]!.trim()]));
}

/** Resolves the E2E target, permitting plaintext HTTP only for a local loopback Worker. */
export function resolveRunnerURL(
  environment: Record<string, string | undefined>,
  info: (message: string) => void = (message) => process.stdout.write(`${message}\n`),
): URL {
  const configured = environment.RUNNER_URL?.trim();
  if (!configured) info(`INFO RUNNER_URL is not set; using ${localRunnerURL}.`);
  const runnerURL = new URL(configured || localRunnerURL);
  const loopback = runnerURL.hostname === "localhost" || runnerURL.hostname === "127.0.0.1" || runnerURL.hostname === "[::1]";
  if ((runnerURL.protocol !== "https:" && !(runnerURL.protocol === "http:" && loopback)) || runnerURL.username || runnerURL.password) {
    throw new Error("RUNNER_URL must use HTTPS, except for HTTP localhost or loopback addresses, and must not contain credentials");
  }
  if (configured) info(`INFO Using RUNNER_URL ${runnerURL.origin}.`);
  return runnerURL;
}

/** Returns whether the target is a local Worker that does not require history archival. */
export function isLocalRunnerURL(runnerURL: URL): boolean {
  return runnerURL.hostname === "localhost" || runnerURL.hostname === "127.0.0.1" || runnerURL.hostname === "[::1]";
}

/** Requires a successful workflow to expose valid aggregate token counters. */
export function requireTokenUsage(job: Record<string, unknown>): void {
  const result = job.result;
  if (!result || typeof result !== "object" || Array.isArray(result)) throw new Error("Successful job did not return a result");
  const usage = (result as Record<string, unknown>).usage;
  if (!usage || typeof usage !== "object" || Array.isArray(usage)) throw new Error("Successful job did not return token usage");
  const values = usage as Record<string, unknown>;
  for (const key of ["total_tokens", "input_tokens", "output_tokens"]) {
    if (typeof values[key] !== "number" || !Number.isSafeInteger(values[key]) || (values[key] as number) < 0) throw new Error(`Successful job returned invalid ${key}`);
  }
}

/** Requires a memory-enabled terminal result to prove its canonical snapshot was stored. */
export function requireMemoryPersistence(job: Record<string, unknown>, maxBytes = 25 * 1024 * 1024): void {
  const result = job.result;
  if (!result || typeof result !== "object" || Array.isArray(result)) throw new Error("Memory-enabled job did not return a result");
  const values = result as Record<string, unknown>;
  if (values.memory_error !== undefined) throw new Error("Memory-enabled job returned a memory error");
  const memory = values.memory;
  if (!memory || typeof memory !== "object" || Array.isArray(memory)) throw new Error("Memory-enabled job did not return persistence metadata");
  const metadata = memory as Record<string, unknown>;
  if (typeof metadata.key !== "string" || !/^memory\/[a-z][a-z0-9_-]{0,63}\.sqlite3$/.test(metadata.key) ||
    typeof metadata.size !== "number" || !Number.isSafeInteger(metadata.size) || metadata.size < 1 || metadata.size > maxBytes ||
    typeof metadata.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(metadata.sha256) ||
    typeof metadata.etag !== "string" || metadata.etag.length === 0 ||
    typeof metadata.persisted_at !== "string" || !Number.isFinite(Date.parse(metadata.persisted_at))) {
    throw new Error("Memory-enabled job returned invalid persistence metadata");
  }
}

/** Rejects an active R2 workflow that does not match the local YAML and prompts. */
export function requireWorkflowVersion(created: Record<string, unknown>, expected: string): void {
  if (created.workflow_version !== expected) {
    throw new Error(`Active workflow ${String(created.workflow_version)} does not match local bundle ${expected}; run pnpm bundle:sync`);
  }
}

/** Returns true once history archival is complete and rejects failed or malformed state. */
export function requireEventDelivery(job: Record<string, unknown>): boolean {
  const delivery = job.history;
  if (!delivery || typeof delivery !== "object" || Array.isArray(delivery)) {
    throw new Error("Terminal job did not return event delivery state");
  }
  const state = (delivery as Record<string, unknown>).state;
  if (state === "archived") return true;
  if (state === "pending" || state === "retrying") return false;
  if (state === "failed") throw new Error("history archival failed");
  if (state === "disabled") throw new Error("history archival is disabled");
  throw new Error("Terminal job returned invalid event delivery state");
}

/** Submits the active workflow and polls until it reaches a terminal state. */
async function main(): Promise<void> {
  if (process.argv.slice(2).some((value: string) => value !== "--")) throw new Error("The workflow is configured in config/workflow.yaml; pnpm e2e accepts no prompt or harness arguments");
  const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  const config = await loadWorkflowConfig(root);
  const localBundle = await buildWorkflowBundle(config, root);
  const environments = await loadConfigurationEnvironments(root);
  const environment = { ...environments.secrets, ...environments.cloudflare };
  const required = requireEnvironment([
    ...requiredSecretNames(config),
    ...(Object.values(config.providers).some((provider) => provider.endpoint.kind === "cloudflare-ai") ? ["CLOUDFLARE_ACCOUNT_ID"] : []),
  ], environment);
  const runnerURL = resolveRunnerURL(environment);
  const authorization = `Bearer ${required[config.api.auth_secret]}`;
  const requireDelivery = true;
  if (requireDelivery) {
    await requireRunnerPreflight(runnerURL, required[config.api.auth_secret]!);
    process.stdout.write(`${JSON.stringify({ phase: "preflight", http_status: 200, status: "ready" })}\n`);
  }
  const client=new FlowClient(runnerURL.origin,required[config.api.auth_secret]!);
  const created = await client.run() as unknown as Record<string,unknown>;
  const createResponse={status:202};
  process.stdout.write(`${JSON.stringify({ phase: "submitted", http_status: createResponse.status, ...created })}\n`);
  if (createResponse.status !== 202 || typeof created.status_url !== "string") throw new Error(`Job submission failed with HTTP ${createResponse.status}`);
  try {
    requireWorkflowVersion(created, localBundle.manifest.digest);
  } catch (error) {
    await fetch(new URL(created.status_url, runnerURL), { method: "DELETE", headers: { authorization } });
    throw error;
  }

  const workflowDeadline = Date.now() + durationToMilliseconds(config.workflow.workflow_timeout) + 120_000;
  let deliveryDeadline: number | null = null;
  let terminalStatus: string | null = null;
  while (true) {
    const deadline = deliveryDeadline ?? workflowDeadline;
    if (Date.now() >= deadline) {
      throw new Error(deliveryDeadline === null ? "Timed out waiting for the deployed workflow" : "Timed out waiting for history archival");
    }
    await new Promise((resolve) => setTimeout(resolve, 2_000));
    const response = await fetch(new URL(created.status_url, runnerURL), { headers: { authorization } });
    const job = await response.json() as Record<string, unknown>;
    process.stdout.write(`${JSON.stringify({ phase: "polled", http_status: response.status, ...job })}\n`);
    if (!response.ok) throw new Error(`Job polling failed with HTTP ${response.status}`);
    if (typeof job.status === "string" && terminalStatuses.has(job.status)) {
      if (terminalStatus === null) {
        terminalStatus = job.status;
        if (config.workflow.memory) requireMemoryPersistence(job, config.memory.max_database_bytes);
        if (job.status === "succeeded" && config.workflow.steps.some(step=>"prompt" in step)) requireTokenUsage(job);
        deliveryDeadline = Date.now() + eventDeliveryTimeoutMs;
      }
      if (!requireDelivery || requireEventDelivery(job)) {
        if (terminalStatus !== "succeeded") process.exitCode = 2;
        return;
      }
    }
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => { console.error(error instanceof Error ? error.message : error); process.exitCode = 1; });
}
