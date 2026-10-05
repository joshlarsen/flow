#!/usr/bin/env node

import { constants } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  rename,
  rm,
} from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  loadConfigurationEnvironments,
  loadRunnerConfig,
  loadWorkflowConfig,
  requiredSecretNames,
  resolveRuntimeEnvironment,
  resolveDeploymentTarget,
  toRuntimeConfig,
} from "./config.ts";
import {
  isEnvironmentProfile,
  unselectedEnvironmentFiles,
  profileEnvironmentFiles,
  readActiveEnvironmentProfile,
  type EnvironmentProfile,
} from "./environment-files.ts";

type EnvironmentAction = EnvironmentProfile | "status";
type RunCommand = (command: string, args: string[]) => Promise<number>;

interface EnvironmentCommandOptions {
  validate?: (root: string, profile: EnvironmentProfile) => Promise<void>;
  run?: RunCommand;
  output?: (message: string) => void;
}

async function regularFile(filename: string): Promise<boolean> {
  try {
    const metadata = await lstat(filename);
    if (!metadata.isFile() || metadata.isSymbolicLink())
      throw new Error(`${filename} must be a regular file`);
    if (metadata.size > 1024 * 1024)
      throw new Error(`${filename} exceeds 1 MiB`);
    return true;
  } catch (error) {
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ENOENT"
    )
      return false;
    throw error;
  }
}

/** Initializes an isolated profile from the shipped examples exactly once. */
export async function ensureEnvironmentProfile(
  root: string,
  profile: EnvironmentProfile,
): Promise<void> {
  const target = profileEnvironmentFiles(root, profile);
  if (!(await regularFile(target.runtime)))
    await copyFile(
      path.join(root, "config/env.example"),
      target.runtime,
      constants.COPYFILE_EXCL,
    );
  const directory = path.dirname(target.secrets);
  let directoryExists = false;
  try {
    const metadata = await lstat(directory);
    if (!metadata.isDirectory() || metadata.isSymbolicLink())
      throw new Error(`${directory} must be a regular directory`);
    directoryExists = true;
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
  }

  if (!directoryExists) {
    const parent = path.dirname(directory);
    await mkdir(parent, { recursive: true });
    const temporary = await mkdtemp(path.join(parent, ".local-"));
    try {
      await copyFile(
        path.join(root, "config/env.secrets.example"),
        path.join(temporary, ".env.secrets"),
        constants.COPYFILE_EXCL,
      );
      await chmod(path.join(temporary, ".env.secrets"), 0o600);
      await copyFile(
        path.join(root, `config/env.${profile}.cloudflare.example`),
        path.join(temporary, ".env.cloudflare"),
        constants.COPYFILE_EXCL,
      );
      await rename(temporary, directory);
    } catch (error) {
      await rm(temporary, { recursive: true, force: true });
      throw error;
    }
  }

  if (
    !(await regularFile(target.secrets)) ||
    !(await regularFile(target.cloudflare))
  ) {
    throw new Error(
      `Environment profile ${profile} requires ${target.secrets} and ${target.cloudflare}`,
    );
  }
  await chmod(target.secrets, 0o600);
}

/** Writes the active-profile marker atomically after candidate validation succeeds. */
export async function activateEnvironmentProfile(
  root: string,
  profile: EnvironmentProfile,
): Promise<void> {
  const marker = path.join(root, ".env.active");
  try {
    const metadata = await lstat(marker);
    if (!metadata.isFile() || metadata.isSymbolicLink())
      throw new Error(`${marker} must be a regular file`);
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
  }
  const temporary = path.join(root, `.env.active.${randomUUID()}.tmp`);
  let handle;
  try {
    handle = await open(temporary, "wx", 0o600);
    await handle.writeFile(`${profile}\n`, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporary, marker);
  } catch (error) {
    await handle?.close().catch(() => undefined);
    await rm(temporary, { force: true });
    throw error;
  }
}

/** Validates a profile against the effective catalog and workflow without changing active state. */
export async function validateEnvironmentProfile(
  root: string,
  profile: EnvironmentProfile,
): Promise<void> {
  const [config, environments] = await Promise.all([
    loadWorkflowConfig(root),
    loadConfigurationEnvironments(root, profile),
  ]);
  const runtime = toRuntimeConfig(config, environments.cloudflare, profile);
  resolveRuntimeEnvironment(config, environments.runtime);
  const missingSecrets = requiredSecretNames(config).filter(
    (name) => !environments.secrets[name]?.trim(),
  );
  if (missingSecrets.length > 0)
    throw new Error(
      `Missing required secrets in ${environments.files.secrets}: ${missingSecrets.join(", ")}`,
    );
  if (!runtime.runnerUrl || !runtime.runnerUrl.startsWith("https://"))
    throw new Error("RUNNER_URL must be the public HTTPS callback origin");
  const apiToken = environments.secrets[config.api.auth_secret];
  if (!apiToken || apiToken.length < 32)
    throw new Error(
      `${config.api.auth_secret} must contain at least 32 characters`,
    );
  if (!runtime.cloudflareAccountId)
    throw new Error(
      `Missing CLOUDFLARE_ACCOUNT_ID in ${environments.files.cloudflare}`,
    );
  if (
    profile === "local" &&
    !environments.cloudflare.CLOUDFLARE_TUNNEL_NAME?.trim()
  ) {
    throw new Error(
      `Missing CLOUDFLARE_TUNNEL_NAME in ${environments.files.cloudflare}`,
    );
  }
  if (profile === "prod") {
    const configured = environments.cloudflare.RUNNER_URL?.trim();
    if (!configured)
      throw new Error(`Missing RUNNER_URL in ${environments.files.cloudflare}`);
    const runnerURL = new URL(configured);
    if (
      runnerURL.protocol !== "https:" ||
      runnerURL.username ||
      runnerURL.password ||
      runnerURL.pathname !== "/" ||
      runnerURL.search ||
      runnerURL.hash
    ) {
      throw new Error(
        `RUNNER_URL in ${environments.files.cloudflare} must be an HTTPS origin`,
      );
    }
  }
}

function run(command: string, args: string[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
}

function relative(root: string, filename: string): string {
  return path.relative(root, filename) || ".";
}

/** Selects an environment and coordinates the production synchronization workflow. */
export async function runEnvironmentCommand(
  action: EnvironmentAction,
  root = process.cwd(),
  options: EnvironmentCommandOptions = {},
): Promise<void> {
  const output =
    options.output ??
    ((message: string) => process.stdout.write(`${message}\n`));
  if (action === "status") {
    const active = await readActiveEnvironmentProfile(root);
    const files = active
      ? profileEnvironmentFiles(root, active)
      : unselectedEnvironmentFiles(root);
    output(`Environment: ${active ?? "unselected"}`);
    try {
      const target = resolveDeploymentTarget(
        await loadRunnerConfig(root),
        active,
      );
      output(`Worker: ${target.deploymentName}`);
      output(`Storage: ${target.storageBucketName}`);
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
    }
    output(`Secrets: ${relative(root, files.secrets)}`);
    output(`Cloudflare: ${relative(root, files.cloudflare)}`);
    output(`Runtime: ${relative(root, files.runtime)} (shared)`);
    return;
  }

  await ensureEnvironmentProfile(root, action);
  if (action === "prod" || options.validate)
    await (options.validate ?? validateEnvironmentProfile)(root, action);
  await activateEnvironmentProfile(root, action);
  output(`Activated ${action} environment profile.`);
  if (action === "local") return;

  if (!options.run) {
    const { buildWorkflowBundle } = await import("./workflow-bundle.ts");
    await buildWorkflowBundle(await loadWorkflowConfig(root), root);
    for (const [command, args] of [
      ["docker", ["version"]],
      ["crane", ["version"]],
      ["tar", ["--version"]],
    ] as const) {
      const code = await run(command, [...args]);
      if (code !== 0)
        throw new Error(`${command} preflight failed before synchronization`);
    }
  }
  const runCommand = options.run ?? run;
  const syncCode = await runCommand("pnpm", ["secrets:sync"]);
  if (syncCode !== 0)
    throw new Error(
      `Production secret synchronization failed with exit code ${syncCode}; prod remains active`,
    );
  const deployCode = await runCommand("pnpm", ["run", "deploy"]);
  if (deployCode !== 0)
    throw new Error(
      `Production deployment failed with exit code ${deployCode}; prod remains active`,
    );
  output(
    "Production secrets, Worker, container, and workflow are synchronized.",
  );
}

function parseAction(argv: string[]): EnvironmentAction {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  if (
    args.length !== 1 ||
    (args[0] !== "status" && !isEnvironmentProfile(args[0] ?? ""))
  ) {
    throw new Error("Usage: pnpm env:local | pnpm env:prod | pnpm env:status");
  }
  return args[0] as EnvironmentAction;
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  runEnvironmentCommand(parseAction(process.argv.slice(2))).catch(
    (error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exitCode = 1;
    },
  );
}
