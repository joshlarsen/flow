#!/usr/bin/env node

import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { spawn } from "node:child_process";

const versionPattern = /^(?:latest|next|\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)$/;
const knownCrashPattern = /Bun has crashed|MemoryExhaustion|Segmentation fault|abort\(\) called|SIG(?:SEGV|ABRT|ILL)|qemu: uncaught target signal/i;

export type ProbeStatus = "compatible" | "known_incompatible" | "error";

export interface CommandResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  timedOut: boolean;
}

export interface ProbeResult {
  status: ProbeStatus;
  detail: string;
}

interface ProbeOptions {
  acpVersion: string;
}

interface InstalledVersions {
  acp: string;
  agentSdk: string;
}

interface RunOptions {
  timeoutMs?: number;
}

const probeCommand = String.raw`
set -eu
entry=$(find /opt/acp/node_modules -type f -path '*/claude-agent-sdk-linux-x64/claude' -print -quit)
if [ -z "$entry" ] || [ ! -x "$entry" ]; then
  echo "could not locate ACP's bundled linux-x64 Claude executable" >&2
  exit 2
fi
ulimit -c 0
exec timeout --signal=KILL 30s "$entry" --version
`;

/** Runs a child process with bounded output and execution time. */
async function run(command: string, args: string[], options: RunOptions = {}): Promise<CommandResult> {
  return new Promise((resolve) => {
    const child = spawn(command, args, { stdio: ["pipe", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, options.timeoutMs ?? 120_000);

    child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
    child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
    child.once("error", (error) => {
      clearTimeout(timer);
      resolve({ code: null, signal: null, stdout, stderr: `${stderr}${error.message}`, timedOut });
    });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal, stdout, stderr, timedOut });
    });
    child.stdin.end();
  });
}

/** Classifies the direct binary launch and recognizes the known Bun/QEMU failure. */
export function classifyProbe(result: CommandResult): ProbeResult {
  const output = `${result.stdout}\n${result.stderr}`.trim();
  if (result.code === 0 && !result.timedOut) {
    return { status: "compatible", detail: result.stdout.trim().split("\n").at(-1) || "exited successfully" };
  }
  if (knownCrashPattern.test(output)) {
    const crash = output.split("\n").find((line) => knownCrashPattern.test(line));
    return { status: "known_incompatible", detail: crash?.trim() || "known Bun/QEMU crash" };
  }
  if (result.timedOut) return { status: "error", detail: "probe timed out" };
  const detail = output.split("\n").filter(Boolean).at(-1) || `exited with ${result.code ?? result.signal ?? "unknown status"}`;
  return { status: "error", detail };
}

/** Maps the direct binary result to a process exit code. */
export function probeExitCode(result: ProbeResult): number {
  if (result.status === "compatible") return 0;
  if (result.status === "known_incompatible") return 1;
  return 2;
}

/** Extracts the default ACP pin used by the production image. */
export function pinnedACPVersionFromDockerfile(contents: string): string {
  const match = contents.match(/^ARG CLAUDE_ACP_VERSION=([^\s]+)$/m);
  const version = match?.[1];
  if (!version || !versionPattern.test(version)) throw new Error("Dockerfile does not contain a valid CLAUDE_ACP_VERSION pin");
  return version;
}

/** Parses a single optional ACP version without accepting arbitrary npm specs. */
export function parseProbeOptions(args: string[], defaultACPVersion: string): ProbeOptions {
  const options: ProbeOptions = { acpVersion: defaultACPVersion };
  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    if (argument === "--") continue;
    if (argument !== "--acp-version") throw new Error(`Unknown argument: ${argument}`);
    const value = args[index + 1];
    if (!value || !versionPattern.test(value)) throw new Error("--acp-version requires a semver, latest, or next");
    options.acpVersion = value;
    index += 1;
  }
  return options;
}

/** Runs ACP's bundled amd64 Claude binary through the active Docker backend. */
async function main(): Promise<void> {
  const pinnedVersion = pinnedACPVersionFromDockerfile(await readFile("Dockerfile", "utf8"));
  const options = parseProbeOptions(process.argv.slice(2), pinnedVersion);
  const container = `claude-amd64-probe-${randomUUID()}`;
  const architectureResult = await run("docker", ["info", "--format", "{{.Architecture}}"], { timeoutMs: 15_000 });
  if (architectureResult.code !== 0) throw new Error(`docker info failed: ${architectureResult.stderr.trim()}`);
  const architecture = architectureResult.stdout.trim().toLowerCase();
  if (!["amd64", "x86_64", "arm64", "aarch64"].includes(architecture)) {
    throw new Error(`Unsupported Docker server architecture: ${architecture}`);
  }

  try {
    const create = await run("docker", [
      "run", "--detach", "--platform", "linux/amd64", "--name", container,
      "--network", "bridge", "--memory", "1g", "--cpus", "1", "--pids-limit", "256", "--ulimit", "core=0",
      "node:24-bookworm-slim", "sleep", "infinity",
    ], { timeoutMs: 60_000 });
    if (create.code !== 0) throw new Error(`probe container creation failed:\n${create.stderr.trim()}`);

    const setup = await run("docker", [
      "exec", "-e", `ACP_VERSION=${options.acpVersion}`, container, "bash", "-lc", String.raw`
set -eu
npm install --prefix /opt/acp "@agentclientprotocol/claude-agent-acp@$ACP_VERSION"
npm cache clean --force
`,
    ], { timeoutMs: 10 * 60_000 });
    if (setup.code !== 0) throw new Error(`probe dependency setup failed:\n${setup.stderr.trim()}`);
    const disconnect = await run("docker", ["network", "disconnect", "bridge", container], { timeoutMs: 15_000 });
    if (disconnect.code !== 0) throw new Error(`could not isolate probe network: ${disconnect.stderr.trim()}`);

    const metadata = await run("docker", ["exec", container, "node", "-e", String.raw`
const read = (file) => JSON.parse(require("node:fs").readFileSync(file, "utf8")).version;
console.log(JSON.stringify({
  acp: read("/opt/acp/node_modules/@agentclientprotocol/claude-agent-acp/package.json"),
  agentSdk: read("/opt/acp/node_modules/@anthropic-ai/claude-agent-sdk/package.json"),
}));
`], { timeoutMs: 30_000 });
    if (metadata.code !== 0) throw new Error(`could not inspect installed versions: ${metadata.stderr.trim()}`);
    const versions = JSON.parse(metadata.stdout) as InstalledVersions;

    const command = await run("docker", [
      "exec", "--user", "10001:10001", "-e", "HOME=/tmp", "-e", "TMPDIR=/tmp",
      container, "bash", "-lc", probeCommand,
    ], { timeoutMs: 45_000 });
    const result = classifyProbe(command);
    console.log(`Docker server: ${architecture}; target: linux/amd64`);
    console.log(`Versions: ACP ${versions.acp}, Agent SDK ${versions.agentSdk}`);
    console.log(`bundled Claude: ${result.status} (${result.detail})`);
    if (result.status === "known_incompatible") {
      console.error("On Apple Silicon, use Apple Virtualization Framework with Rosetta enabled; Docker VMM does not support Rosetta.");
    }
    process.exitCode = probeExitCode(result);
  } finally {
    await run("docker", ["rm", "--force", container], { timeoutMs: 30_000 });
  }
}

if (process.argv[1]?.endsWith("claude-amd64-probe.ts")) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 2;
  });
}
