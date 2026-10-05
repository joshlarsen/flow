#!/usr/bin/env node
import { createWriteStream } from "node:fs";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadConfigurationEnvironments, loadRunnerConfig } from "./config.ts";
import { FlowClient } from "./flow-client.ts";
const usage =
  "Usage: pnpm flow <preflight|run|wait|list|get|cancel|events|traces|metrics|artifact-download> [job-id] [path] --json --wait --timeout <seconds> --idempotency-key <key> --cursor <cursor> --limit <1..200> --output <file>";
export async function main(argv = process.argv.slice(2)) {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  if (!args.length || args.includes("--help")) {
    process.stdout.write(`${usage}\n`);
    return;
  }
  const flags = new Map<string, string | boolean>(),
    positionals: string[] = [];
  for (let i = 0; i < args.length; i++) {
    const value = args[i]!;
    if (value.startsWith("--")) {
      if (["--json", "--wait"].includes(value)) flags.set(value, true);
      else if (
        [
          "--timeout",
          "--idempotency-key",
          "--cursor",
          "--limit",
          "--output",
          "--url",
        ].includes(value)
      ) {
        if (!args[i + 1] || args[i + 1]!.startsWith("--"))
          throw new Error(`Missing ${value} value`);
        flags.set(value, args[++i]!);
      } else throw new Error(`Unknown option ${value}`);
    } else positionals.push(value);
  }
  const config = await loadRunnerConfig(),
    environment = await loadConfigurationEnvironments();
  const token = environment.secrets[config.api.auth_secret];
  if (!token)
    throw new Error(`Missing ${config.api.auth_secret} in active profile`);
  const origin = flags.get("--url") ?? environment.cloudflare.RUNNER_URL;
  if (typeof origin !== "string")
    throw new Error("Configure RUNNER_URL or pass --url");
  const client = new FlowClient(origin, token),
    [command, id, artifactPath] = positionals;
  const timeout = Number(flags.get("--timeout") ?? 3600) * 1000,
    limit = Number(flags.get("--limit") ?? 100);
  if (
    !Number.isFinite(timeout) ||
    timeout <= 0 ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 200
  )
    throw new Error("Invalid timeout or limit");
  if (!["preflight", "run", "list"].includes(command!) && !id)
    throw new Error("A job ID is required");
  let result: unknown;
  switch (command) {
    case "preflight":
      result = await client.preflight();
      break;
    case "run":
      await client.preflight();
      result = await client.run(
        flags.get("--idempotency-key") as string | undefined,
      );
      if (flags.get("--wait"))
        result = await client.wait(
          (result as { job_id: string }).job_id,
          timeout,
        );
      break;
    case "wait":
      result = await client.wait(id!, timeout);
      break;
    case "get":
      result = await client.get(id!);
      break;
    case "cancel":
      result = await client.cancel(id!);
      break;
    case "list":
      result = await client.page(
        "jobs",
        undefined,
        flags.get("--cursor") as string | undefined,
        limit,
      );
      break;
    case "events":
    case "traces":
    case "metrics":
      result = await client.page(
        command,
        id,
        flags.get("--cursor") as string | undefined,
        limit,
      );
      break;
    case "artifact-download": {
      const output = flags.get("--output");
      if (!artifactPath || typeof output !== "string")
        throw new Error("Artifact path and --output are required");
      const response = await client.artifact(id!, artifactPath);
      if (!response.body) throw new Error("Artifact response has no body");
      await pipeline(
        Readable.fromWeb(
          response.body as import("node:stream/web").ReadableStream,
        ),
        createWriteStream(output, { flags: "wx" }),
      );
      result = { saved: output };
      break;
    }
    default:
      throw new Error(usage);
  }
  process.stdout.write(
    `${JSON.stringify(result, null, flags.get("--json") ? undefined : 2)}\n`,
  );
  if (command === "wait" || (command === "run" && flags.get("--wait"))) {
    if ((result as { status: string }).status !== "succeeded")
      process.exitCode = 2;
  }
}
if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
)
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
