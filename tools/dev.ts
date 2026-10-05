import { spawn } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadConfigurationEnvironments } from "./config.ts";
import {
  readActiveEnvironmentProfile,
  requireEnvironmentProfile,
  type EnvironmentProfile,
} from "./environment-files.ts";
import { validateEnvironmentProfile } from "./environment.ts";
import { generate } from "./generate-config.ts";

interface CommandResult {
  code: number;
  stdout: string;
  stderr: string;
}

type CaptureCommand = (
  command: string,
  args: string[],
) => Promise<CommandResult>;
type RunCommand = (command: string, args: string[]) => Promise<number>;

interface DevEnvironmentFiles {
  runtime: string;
  secrets: string;
  cloudflare: string;
}

interface EnsureDevTunnelOptions {
  cloudflareFile?: string;
  captureCommand?: CaptureCommand;
  runCommand?: RunCommand;
  info?: (message: string) => void;
}

const unselectedEnvironmentFiles: DevEnvironmentFiles = {
  runtime: ".env",
  secrets: ".env.secrets",
  cloudflare: ".env.cloudflare",
};

/** Prevents local Wrangler from loading production bindings or credentials. */
export function assertLocalDevelopmentProfile(
  profile: EnvironmentProfile | null,
): asserts profile is "local" {
  requireEnvironmentProfile(profile, "local", "pnpm dev");
}

/** Resolves the existing named tunnel that Wrangler must use for local development. */
export function resolveDevTunnelName(
  environment: Record<string, string | undefined>,
): string {
  const name = environment.CLOUDFLARE_TUNNEL_NAME?.trim();
  if (!name) {
    throw new Error(
      "Missing CLOUDFLARE_TUNNEL_NAME in .env.cloudflare; configure an existing named tunnel before running pnpm dev",
    );
  }
  return name;
}

/** Builds the fixed Wrangler invocation while retaining supported CLI passthrough arguments. */
export function devWranglerArguments(
  tunnelName: string,
  extraArguments: string[] = [],
  files: DevEnvironmentFiles = unselectedEnvironmentFiles,
): string[] {
  const passthrough =
    extraArguments[0] === "--" ? extraArguments.slice(1) : extraArguments;
  const hasPort = passthrough.some(
    (argument) => argument === "--port" || argument.startsWith("--port="),
  );
  return [
    "dev",
    "--tunnel",
    "--tunnel-name",
    tunnelName,
    ...(hasPort ? [] : ["--port", "8787"]),
    "--env-file",
    files.cloudflare,
    "--env-file",
    files.secrets,
    "--env-file",
    files.runtime,
    "-c",
    ".generated/wrangler.jsonc",
    ...passthrough,
  ];
}

/** Builds the Wrangler invocation used to look up the configured tunnel. */
export function tunnelInfoArguments(
  tunnelName: string,
  cloudflareFile = ".env.cloudflare",
): string[] {
  return [
    "tunnel",
    "info",
    tunnelName,
    "--env-file",
    cloudflareFile,
    "-c",
    ".generated/wrangler.jsonc",
  ];
}

/** Builds the Wrangler invocation used for one-time tunnel creation. */
export function tunnelCreateArguments(
  tunnelName: string,
  cloudflareFile = ".env.cloudflare",
): string[] {
  return [
    "tunnel",
    "create",
    tunnelName,
    "--env-file",
    cloudflareFile,
    "-c",
    ".generated/wrangler.jsonc",
  ];
}

function capture(command: string, args: string[]): Promise<CommandResult> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.once("error", reject);
    child.once("exit", (code) =>
      resolve({
        code: code ?? 1,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      }),
    );
  });
}

function run(command: string, args: string[]): Promise<number> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
}

/** Creates the configured tunnel only when Wrangler confirms that it is absent. */
export async function ensureDevTunnel(
  tunnelName: string,
  options: EnsureDevTunnelOptions = {},
): Promise<boolean> {
  const cloudflareFile = options.cloudflareFile ?? ".env.cloudflare";
  const captureCommand = options.captureCommand ?? capture;
  const runCommand = options.runCommand ?? run;
  const info =
    options.info ?? ((message: string) => process.stdout.write(`${message}\n`));
  const inspected = await captureCommand(
    "wrangler",
    tunnelInfoArguments(tunnelName, cloudflareFile),
  );
  if (inspected.code === 0) return false;
  const output = `${inspected.stdout}\n${inspected.stderr}`;
  if (
    !output.includes("is neither the ID nor the name of any of your tunnels")
  ) {
    throw new Error(
      `Unable to inspect Cloudflare Tunnel ${JSON.stringify(tunnelName)}:\n${output.trim()}`,
    );
  }
  info(
    `Cloudflare Tunnel ${JSON.stringify(tunnelName)} does not exist; creating it.`,
  );
  const created = await runCommand(
    "wrangler",
    tunnelCreateArguments(tunnelName, cloudflareFile),
  );
  if (created !== 0)
    throw new Error(
      `Unable to create Cloudflare Tunnel ${JSON.stringify(tunnelName)}`,
    );
  return true;
}

/** Starts Wrangler with an authenticated named tunnel and mirrors its exit status. */
export async function dev(
  root = process.cwd(),
  extraArguments: string[] = process.argv.slice(2),
): Promise<number> {
  const activeProfile = await readActiveEnvironmentProfile(root);
  assertLocalDevelopmentProfile(activeProfile);
  if (
    !extraArguments.some((argument) =>
      ["--help", "-h", "--version", "-v"].includes(argument),
    )
  )
    await validateEnvironmentProfile(root, activeProfile);
  await generate(root, activeProfile);
  const environments = await loadConfigurationEnvironments(root, activeProfile);
  const tunnelName = resolveDevTunnelName(environments.cloudflare);
  if (
    !extraArguments.some((argument) =>
      ["--help", "-h", "--version", "-v"].includes(argument),
    )
  ) {
    await ensureDevTunnel(tunnelName, {
      cloudflareFile: environments.files.cloudflare,
    });
  }
  return run(
    "wrangler",
    devWranglerArguments(tunnelName, extraArguments, environments.files),
  );
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  dev()
    .then((code) => {
      process.exitCode = code;
    })
    .catch((error: unknown) => {
      console.error(error instanceof Error ? error.message : error);
      process.exitCode = 1;
    });
}
