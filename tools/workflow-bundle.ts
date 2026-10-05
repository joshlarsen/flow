import { createHash, randomUUID } from "node:crypto";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, realpath, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import type { DeploymentTarget, ReasoningEffort, RequiredMetric, WorkflowRunnerConfig } from "./config.ts";
import { durationToMilliseconds } from "./config.ts";
import {
  activeWorkflowKey,
  workflowBundleArchiveKey,
  workflowBundleManifestKey,
} from "../src/storage-keys.ts";

export type WorkflowBundleFileKind = "prompt" | "script" | "skill";

export interface BundledWorkflow {
  version: 1;
  name: string;
  memory_enabled: boolean;
  workflow_timeout_ms: number;
  token_budget?: { limit: number; period: "day" | "week" };
  default_step_timeout_ms: number;
  default_harness?: string | null;
  default_model?: string | null;
  default_reasoning_effort?: ReasoningEffort | null;
  steps: Array<{
    id: string;
    prompt: string;
    allow_user_input: boolean;
    harness: string;
    provider: string;
    model: string;
    reasoning_effort: ReasoningEffort | null;
    required_metrics: RequiredMetric[];
    timeout_ms: number;
  } | {
    id: string;
    command: string[];
    timeout_ms: number;
  }>;
}

export interface WorkflowBundleFile {
  kind: WorkflowBundleFileKind;
  path: string;
  size: number;
  sha256: string;
  executable: boolean;
}

export interface WorkflowBundleManifest {
  version: 1;
  digest: string;
  sort_key: string;
  workflow: BundledWorkflow;
  archive: { key: string; size: number; sha256: string };
  files: WorkflowBundleFile[];
  total_bytes: number;
}

/** Builds the deterministic instructions appended to prompts with required metrics. */
export function requiredMetricInstructions(metrics: RequiredMetric[]): string {
  if (metrics.length === 0) return "";
  const assignments = metrics.map((metric) => `${metric.namespace}.${metric.key}=<number>`);
  return [
    "", "", "## Required metrics", "",
    "Before completing this step, call the `metrics` MCP server's `emit` tool (`mcp.metrics.emit`). Pass a `metrics` array containing `namespace.key=value` strings with finite numeric values.",
    "", "Emit all of these required metrics:", "",
    ...metrics.map((metric) => `- \`${metric.namespace}.${metric.key}\`: ${metric.description}`),
    "", "Example arguments:", "```json", JSON.stringify({ metrics: assignments }), "```",
  ].join("\n");
}

interface CollectedFile extends WorkflowBundleFile {
  bytes: Buffer;
}

interface GitContext {
  root: string;
  tracked: Set<string>;
}

const encoder = new TextEncoder();
const promptPathPattern = /^[A-Za-z0-9][A-Za-z0-9._/-]*\.md$/;

function compareText(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

function sha256(value: string | Uint8Array): string {
  return createHash("sha256").update(value).digest("hex");
}

function bundleSortKey(now = new Date()): string {
  return now.toISOString().replaceAll("-", "").replaceAll(":", "");
}

function isWithin(root: string, candidate: string): boolean {
  const relative = path.relative(root, candidate);
  return relative === "" || (!relative.startsWith(`..${path.sep}`) && relative !== ".." && !path.isAbsolute(relative));
}

function safeBundlePath(value: string): boolean {
  if (!value || value.startsWith("/") || value.includes("\\") || encoder.encode(value).byteLength > 1024) return false;
  return value.split("/").every((part) => part !== "" && part !== "." && part !== ".." && !part.includes("\0") && encoder.encode(part).byteLength <= 255);
}

function run(command: string, args: string[], environment?: NodeJS.ProcessEnv, cwd?: string): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit", env: environment, cwd });
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
}

function capture(command: string, args: string[], cwd: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, stdio: ["ignore", "pipe", "pipe"] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.once("error", reject);
    child.once("exit", (code) => code === 0
      ? resolve(Buffer.concat(stdout).toString("utf8"))
      : reject(new Error(Buffer.concat(stderr).toString("utf8").trim() || `${command} exited with ${code ?? 1}`)));
  });
}

/** Loads the repository boundary and tracked paths used to constrain followed symlinks. */
async function loadGitContext(root: string): Promise<GitContext> {
  const gitRoot = (await capture("git", ["rev-parse", "--show-toplevel"], root)).trim();
  const output = await capture("git", ["ls-files", "-z"], gitRoot);
  return { root: path.resolve(gitRoot), tracked: new Set(output.split("\0").filter(Boolean)) };
}

/** Collects a bundle namespace while dereferencing only repository-confined symlinks. */
async function collectTree(
  projectRoot: string,
  kind: WorkflowBundleFileKind,
  required: boolean,
): Promise<CollectedFile[]> {
  const sourceName = kind === "prompt" ? "prompts" : `${kind}s`;
  const sourceRoot = path.join(projectRoot, "config", sourceName);
  try {
    const rootInfo = await lstat(sourceRoot);
    if (!rootInfo.isDirectory() && !rootInfo.isSymbolicLink()) throw new Error(`${sourceRoot} is not a directory`);
  } catch (error) {
    if (!required && error && typeof error === "object" && "code" in error && error.code === "ENOENT") return [];
    throw error;
  }
  const configRoot = path.join(projectRoot, "config");
  let gitContext: GitContext | undefined;
  const files: CollectedFile[] = [];

  const walk = async (physical: string, logical: string, ancestors: ReadonlySet<string>, followedLink: boolean): Promise<void> => {
    const info = await lstat(physical);
    let resolved = physical;
    let targetInfo = info;
    let viaLink = followedLink;
    if (info.isSymbolicLink()) {
      gitContext ??= await loadGitContext(projectRoot);
      try {
        resolved = await realpath(physical);
      } catch {
        throw new Error(`Bundle path ${sourceName}/${logical} is a broken symlink`);
      }
      if (!isWithin(gitContext.root, resolved)) throw new Error(`Bundle path ${sourceName}/${logical} escapes the Git root`);
      targetInfo = await stat(resolved);
      viaLink = true;
    }

    if (targetInfo.isDirectory()) {
      const resolvedDirectory = await realpath(resolved);
      if (ancestors.has(resolvedDirectory)) throw new Error(`Bundle path ${sourceName}/${logical || "."} contains a symlink cycle`);
      const nextAncestors = new Set(ancestors).add(resolvedDirectory);
      const entries = await readdir(resolvedDirectory, { withFileTypes: true });
      for (const entry of entries.sort((left, right) => compareText(left.name, right.name))) {
        const childLogical = logical ? `${logical}/${entry.name}` : entry.name;
        await walk(path.join(resolvedDirectory, entry.name), childLogical, nextAncestors, viaLink);
      }
      return;
    }
    if (!targetInfo.isFile()) throw new Error(`Bundle path ${sourceName}/${logical} is not a regular file`);
    if (!safeBundlePath(logical)) throw new Error(`Bundle path ${sourceName}/${logical} is unsafe`);
    if (path.basename(logical).startsWith(".") && targetInfo.size === 0) return;
    if (kind === "prompt" && !logical.endsWith(".md")) return;
    if (kind === "prompt" && (!promptPathPattern.test(logical) || logical.split("/").some((part) => part.startsWith(".")))) throw new Error(`Prompt path ${logical} is invalid`);
    if (viaLink) {
      gitContext ??= await loadGitContext(projectRoot);
      if (!isWithin(gitContext.root, resolved)) throw new Error(`Bundle path ${sourceName}/${logical} escapes the Git root`);
      if (!isWithin(configRoot, resolved)) {
        const repositoryPath = path.relative(gitContext.root, resolved).split(path.sep).join("/");
        if (!gitContext.tracked.has(repositoryPath)) throw new Error(`Bundle path ${sourceName}/${logical} resolves to untracked file ${repositoryPath}`);
      }
    }
    const bytes = await readFile(resolved);
    if (kind === "prompt") {
      if (bytes.byteLength === 0) throw new Error(`Prompt ${logical} must not be empty`);
      new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    }
    files.push({ kind, path: logical, size: bytes.byteLength, sha256: sha256(bytes), executable: (targetInfo.mode & 0o111) !== 0, bytes });
  };

  await walk(sourceRoot, "", new Set(), false);
  return files.sort((left, right) => compareText(left.path, right.path));
}

function compileWorkflow(config: WorkflowRunnerConfig): BundledWorkflow {
  const defaults = config.workflow.defaults;
  const defaultTimeout = durationToMilliseconds(defaults.step_timeout);
  return {
    version: 1,
    name: config.workflow.name,
    memory_enabled: config.workflow.memory,
    workflow_timeout_ms: durationToMilliseconds(config.workflow.workflow_timeout),
    ...(config.workflow.token_budget ? { token_budget: config.workflow.token_budget } : {}),
    default_step_timeout_ms: defaultTimeout,
    default_harness: defaults.harness ?? null,
    default_model: defaults.model ?? null,
    default_reasoning_effort: defaults.reasoning_effort ?? null,
    steps: config.workflow.steps.map((step) => "command" in step
      ? { id: step.id, command: step.command, timeout_ms: step.timeout ? durationToMilliseconds(step.timeout) : defaultTimeout }
      : {
          id: step.id,
          prompt: step.prompt,
          allow_user_input: step.allow_user_input ?? false,
          harness: step.harness ?? defaults.harness!,
          provider: step.provider ?? defaults.provider!,
          model: step.model ?? defaults.model!,
          reasoning_effort: step.reasoning_effort === undefined ? defaults.reasoning_effort ?? null : step.reasoning_effort,
          required_metrics: step.required_metrics,
          timeout_ms: step.timeout ? durationToMilliseconds(step.timeout) : defaultTimeout,
        }),
  };
}

/** Builds the archive and readable manifest retained beneath repository-local tmp/. */
export async function buildWorkflowBundle(config: WorkflowRunnerConfig, root = process.cwd()): Promise<{ manifest: WorkflowBundleManifest; archivePath: string; manifestPath: string }> {
  const [prompts, scripts, skills] = await Promise.all([
    collectTree(root, "prompt", config.workflow.steps.some(step => "prompt" in step)),
    collectTree(root, "script", false),
    collectTree(root, "skill", false),
  ]);
  if (prompts.length > config.runner.max_prompt_files) throw new Error(`Workflow bundle contains more than ${config.runner.max_prompt_files} prompts`);
  if (scripts.length + skills.length > config.runner.max_asset_files) throw new Error(`Workflow bundle contains more than ${config.runner.max_asset_files} assets`);
  for (const file of prompts) if (file.size > config.runner.max_prompt_bytes) throw new Error(`Prompt ${file.path} exceeds max_prompt_bytes`);
  for (const file of [...scripts, ...skills]) if (file.size > config.runner.max_asset_bytes) throw new Error(`${file.kind} ${file.path} exceeds max_asset_bytes`);
  const promptBytes = prompts.reduce((total, file) => total + file.size, 0);
  const assetBytes = [...scripts, ...skills].reduce((total, file) => total + file.size, 0);
  if (promptBytes > config.runner.max_prompt_bundle_bytes) throw new Error("Workflow prompts exceed max_prompt_bundle_bytes");
  if (assetBytes > config.runner.max_asset_bundle_bytes) throw new Error("Workflow assets exceed max_asset_bundle_bytes");
  const promptPaths = new Set(prompts.map((file) => file.path));
  const workflow = compileWorkflow(config);
  for (const step of workflow.steps) if ("prompt" in step && !promptPaths.has(step.prompt)) throw new Error(`Workflow step ${step.id} references missing prompt ${step.prompt}`);
  const promptSizes = new Map(prompts.map((file) => [file.path, file.size]));
  for (const step of workflow.steps) {
    if (!("prompt" in step)) continue;
    const size = promptSizes.get(step.prompt)! + encoder.encode(requiredMetricInstructions(step.required_metrics)).byteLength;
    if (size > config.runner.max_prompt_bytes) throw new Error(`Prompt ${step.prompt} plus required metric instructions for step ${step.id} exceeds max_prompt_bytes`);
  }
  const collected = [...prompts, ...scripts, ...skills];
  const files = collected.map(({ bytes: _bytes, ...file }) => file);
  const digest = sha256(JSON.stringify({ workflow, files }));
  const outputRoot = path.join(root, "tmp");
  const archivePath = path.join(outputRoot, "workflow-bundle.tgz");
  const manifestPath = path.join(outputRoot, "workflow-manifest.json");
  await mkdir(outputRoot, { recursive: true });
  const stage = await mkdtemp(path.join(outputRoot, `.workflow-bundle-stage-${randomUUID()}-`));
  try {
    for (const directory of ["prompts", "scripts", "skills"]) await mkdir(path.join(stage, directory), { recursive: true });
    for (const file of collected) {
      const namespace = file.kind === "prompt" ? "prompts" : `${file.kind}s`;
      const destination = path.join(stage, namespace, ...file.path.split("/"));
      await mkdir(path.dirname(destination), { recursive: true });
      await writeFile(destination, file.bytes, { mode: file.executable ? 0o555 : 0o444 });
      await chmod(destination, file.executable ? 0o555 : 0o444);
      await utimes(destination, 0, 0);
    }
    await rm(archivePath, { force: true });
    const tarCode = await run("tar", ["-czf", archivePath, "-C", stage, "prompts", "scripts", "skills"], {
      ...process.env,
      COPYFILE_DISABLE: "1",
      COPY_EXTENDED_ATTRIBUTES_DISABLE: "1",
    });
    if (tarCode !== 0) throw new Error(`tar exited with ${tarCode}`);
    const archiveBytes = await readFile(archivePath);
    const archiveLimit = config.runner.max_prompt_bundle_bytes + config.runner.max_asset_bundle_bytes + (config.runner.max_prompt_files + config.runner.max_asset_files + 16) * 1024 + 1024 * 1024;
    if (archiveBytes.byteLength > archiveLimit) throw new Error("Workflow archive exceeds its compressed byte limit");
    const archiveDigest = sha256(archiveBytes);
    const sortKey = bundleSortKey();
    const manifest: WorkflowBundleManifest = {
      version: 1,
      digest,
      sort_key: sortKey,
      workflow,
      archive: { key: workflowBundleArchiveKey(sortKey, digest), size: archiveBytes.byteLength, sha256: archiveDigest },
      files,
      total_bytes: promptBytes + assetBytes,
    };
    const manifestText = `${JSON.stringify(manifest, null, 2)}\n`;
    if (encoder.encode(manifestText).byteLength > 2 * 1024 * 1024) throw new Error("Workflow bundle manifest exceeds 2 MiB");
    await writeFile(manifestPath, manifestText);
    return { manifest, archivePath, manifestPath };
  } finally {
    await rm(stage, { recursive: true, force: true });
  }
}

async function wrangler(args: string[], accountId: string): Promise<number> {
  return run("pnpm", ["wrangler", "--cwd", tmpdir(), ...args], { ...process.env, CLOUDFLARE_ACCOUNT_ID: accountId });
}

type WranglerCommand = (args: string[], accountId: string) => Promise<number>;

/** Ensures the deterministic R2 bucket exists. */
export async function ensureStorageBucket(
  target: DeploymentTarget,
  accountId: string,
  command: WranglerCommand = wrangler,
): Promise<string> {
  const bucket = target.storageBucketName;
  if (await command(["r2", "bucket", "info", bucket], accountId) !== 0 && await command(["r2", "bucket", "create", bucket], accountId) !== 0) {
    throw new Error(`Could not create R2 bucket ${bucket}`);
  }
  return bucket;
}

/** Uploads one compressed workflow bundle and optionally activates its readable manifest. */
export async function syncWorkflowBundle(
  config: WorkflowRunnerConfig,
  target: DeploymentTarget,
  accountId: string,
  root = process.cwd(),
  activate = true,
  command: WranglerCommand = wrangler,
): Promise<WorkflowBundleManifest> {
  const built = await buildWorkflowBundle(config, root);
  const bucket = await ensureStorageBucket(target, accountId, command);
  if (await command(["r2", "object", "put", `${bucket}/${built.manifest.archive.key}`, "--remote", "--file", built.archivePath, "--content-type", "application/gzip"], accountId) !== 0) {
    throw new Error("Could not upload workflow bundle archive");
  }
  const manifestKey = workflowBundleManifestKey(built.manifest.sort_key, built.manifest.digest);
  if (await command(["r2", "object", "put", `${bucket}/${manifestKey}`, "--remote", "--file", built.manifestPath, "--content-type", "application/json"], accountId) !== 0) {
    throw new Error("Could not upload workflow bundle manifest");
  }
  if (activate) await activateWorkflowBundle(target, built.manifest, accountId, built.manifestPath, command);
  return built.manifest;
}

/** Atomically publishes an uploaded workflow bundle manifest as active. */
export async function activateWorkflowBundle(
  target: DeploymentTarget,
  manifest: WorkflowBundleManifest,
  accountId: string,
  existingPath?: string,
  command: WranglerCommand = wrangler,
): Promise<void> {
  const bucket = target.storageBucketName;
  let temporary: string | undefined;
  let manifestPath = existingPath;
  try {
    if (!manifestPath) {
      temporary = await mkdtemp(path.join(tmpdir(), "agent-runner-active-"));
      manifestPath = path.join(temporary, "active.json");
      await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);
    }
    if (await command(["r2", "object", "put", `${bucket}/${activeWorkflowKey}`, "--remote", "--file", manifestPath, "--content-type", "application/json"], accountId) !== 0) {
      throw new Error("Could not activate workflow bundle");
    }
  } finally {
    if (temporary) await rm(temporary, { recursive: true, force: true });
  }
}

/** Notifies the deployed Worker after active policy changes so waiting jobs can resume immediately. */
export async function reconcileWorkflowBudget(
  runnerURL: string,
  token: string,
  request: typeof fetch = fetch,
): Promise<void> {
  if (!runnerURL || token.trim().length < 32) throw new Error("RUNNER_URL and the configured API secret are required to reconcile workflow budgets");
  const response = await request(`${runnerURL.replace(/\/$/, "")}/v1/workflow-budget/reconcile`, {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
    signal: AbortSignal.timeout(30_000),
  });
  if (!response.ok) {
    const detail = (await response.text()).trim().slice(0, 1024);
    throw new Error(`Workflow budget reconciliation returned HTTP ${response.status}${detail ? `: ${detail}` : ""}`);
  }
}
