#!/usr/bin/env node

import { mkdtemp, open, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  loadConfigurationEnvironments,
  loadWorkflowConfig,
  resolveDeploymentTarget,
  toRuntimeConfig,
} from "./config.ts";
import {
  readActiveEnvironmentProfile,
  requireEnvironmentProfile,
  type EnvironmentProfile,
} from "./environment-files.ts";
import { validateEnvironmentProfile } from "./environment.ts";
import { generate } from "./generate-config.ts";
import {
  buildWorkflowBundle,
  activateWorkflowBundle,
  reconcileWorkflowBudget,
  syncWorkflowBundle,
} from "./workflow-bundle.ts";

const registryOrigin = "https://registry.cloudflare.com";
const registryChunkBytes = 64 * 1024 * 1024;
const registryRequestAttempts = 5;

interface RegistryCredentials {
  username: string;
  password: string;
}

interface OciDescriptor {
  digest: string;
  size: number;
}

/** Prevents the production deployment command from consuming another profile. */
export function assertProductionDeploymentProfile(
  profile: EnvironmentProfile | null,
): asserts profile is "prod" {
  requireEnvironmentProfile(profile, "prod", "pnpm run deploy");
}

function waitMilliseconds(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/** Runs a child command with inherited output and returns its exit code. */
async function run(command: string, args: string[]): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const child = spawn(command, args, { stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
  });
}

/** Captures a child command's stdout while preserving diagnostic stderr. */
async function capture(command: string, args: string[]): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["ignore", "pipe", "inherit"],
    });
    const output: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => output.push(chunk));
    child.once("error", reject);
    child.once("exit", (code) =>
      code === 0
        ? resolve(Buffer.concat(output).toString("utf8"))
        : reject(new Error(`${command} exited with ${code ?? 1}`)),
    );
  });
}

/** Writes a secret to a child process without exposing it in arguments or output. */
async function runWithInput(
  command: string,
  args: string[],
  input: string,
): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "inherit", "inherit"],
    });
    child.once("error", reject);
    child.once("exit", (code) => resolve(code ?? 1));
    child.stdin.end(input);
  });
}

/** Runs Wrangler outside the repository so local environment files cannot replace its OAuth token. */
async function runWrangler(configPath: string): Promise<number> {
  return run("pnpm", [
    "wrangler",
    "--cwd",
    tmpdir(),
    "deploy",
    "-c",
    configPath,
  ]);
}

/** Obtains a short-lived Cloudflare registry credential without printing it. */
async function registryCredentials(
  configPath: string,
): Promise<RegistryCredentials> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const output = await capture("pnpm", [
        "wrangler",
        "--cwd",
        tmpdir(),
        "containers",
        "registries",
        "credentials",
        "registry.cloudflare.com",
        "--push",
        "--pull",
        "--json",
        "-c",
        configPath,
      ]);
      const value: unknown = JSON.parse(output);
      if (!value || typeof value !== "object" || Array.isArray(value))
        throw new Error("Wrangler returned invalid registry credentials");
      const credentials = value as Record<string, unknown>;
      if (
        typeof credentials.username !== "string" ||
        typeof credentials.password !== "string" ||
        !credentials.username ||
        !credentials.password
      ) {
        throw new Error("Wrangler returned incomplete registry credentials");
      }
      return { username: credentials.username, password: credentials.password };
    } catch (error) {
      lastError = error;
      if (attempt === 3) break;
      process.stderr.write(
        `Cloudflare registry credential request failed; retrying (${attempt}/3).\n`,
      );
      await waitMilliseconds(attempt * 1000);
    }
  }
  const detail =
    lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(
    `Cloudflare registry credential request failed after 3 attempts: ${detail}`,
    { cause: lastError },
  );
}

/** Refreshes crane's credential-store entry without exposing the password in process arguments. */
async function loginCrane(configPath: string): Promise<void> {
  const credentials = await registryCredentials(configPath);
  const code = await runWithInput(
    "crane",
    [
      "auth",
      "login",
      "registry.cloudflare.com",
      "--username",
      credentials.username,
      "--password-stdin",
    ],
    credentials.password,
  );
  if (code !== 0) throw new Error(`crane auth login exited with ${code}`);
}

/** Resolves and constrains an upload location before forwarding registry credentials to it. */
function uploadLocation(current: URL, response: Response): URL {
  const location = response.headers.get("location");
  if (!location)
    throw new Error("Cloudflare registry response omitted the upload location");
  const resolved = new URL(location, current);
  if (resolved.origin !== registryOrigin)
    throw new Error("Cloudflare registry returned an unsafe upload location");
  return resolved;
}

/** Sends one authenticated registry request without allowing credential-bearing redirects. */
async function registryRequest(
  url: URL,
  credentials: RegistryCredentials,
  init: RequestInit,
  fetcher: typeof fetch,
): Promise<Response> {
  const headers = new Headers(init.headers);
  headers.set(
    "authorization",
    `Basic ${Buffer.from(`${credentials.username}:${credentials.password}`).toString("base64")}`,
  );
  return fetcher(url, {
    ...init,
    headers,
    redirect: "error",
    signal: init.signal ?? AbortSignal.timeout(120_000),
  });
}

function transientRegistryStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function registryRetryDelay(
  response: Response | null,
  attempt: number,
): number {
  const retryAfter = response?.headers.get("retry-after")?.trim();
  if (retryAfter && /^\d+$/.test(retryAfter))
    return Math.min(30_000, Number(retryAfter) * 1000);
  return Math.min(5_000, 250 * 2 ** (attempt - 1));
}

/** Retries safe registry requests across transient transport and service failures. */
async function retryRegistryRequest(
  operation: string,
  url: URL,
  credentials: RegistryCredentials,
  init: RequestInit,
  fetcher: typeof fetch,
  wait: (milliseconds: number) => Promise<void>,
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= registryRequestAttempts; attempt++) {
    let response: Response | null = null;
    try {
      response = await registryRequest(url, credentials, init, fetcher);
      if (
        !transientRegistryStatus(response.status) ||
        attempt === registryRequestAttempts
      )
        return response;
      lastError = new Error(`HTTP ${response.status}`);
      await response.body?.cancel();
    } catch (error) {
      lastError = error;
      if (attempt === registryRequestAttempts) break;
    }
    await wait(registryRetryDelay(response, attempt));
  }
  const detail =
    lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(
    `${operation} failed after ${registryRequestAttempts} attempts: ${detail}`,
    { cause: lastError },
  );
}

/** Returns the byte offset recorded by an interrupted registry upload. */
async function recoverUploadOffset(
  location: URL,
  credentials: RegistryCredentials,
  attemptedOffset: number,
  fetcher: typeof fetch,
  wait: (milliseconds: number) => Promise<void>,
): Promise<{ location: URL; offset: number }> {
  const response = await retryRegistryRequest(
    "Cloudflare registry upload status",
    location,
    credentials,
    { method: "GET" },
    fetcher,
    wait,
  );
  if (response.status !== 204)
    throw new Error(
      `Cloudflare registry upload status returned HTTP ${response.status}`,
    );
  const nextLocation = uploadLocation(location, response);
  const range = response.headers.get("range");
  if (!range) return { location: nextLocation, offset: 0 };
  const normalizedRange = range.trim();
  if (/^(?:bytes=)?0--1$/i.test(normalizedRange))
    return { location: nextLocation, offset: 0 };
  const match = /^(?:bytes=)?(\d+)-(\d+)$/i.exec(normalizedRange);
  if (!match || match[1] !== "0")
    throw new Error(
      `Cloudflare registry returned an invalid upload range: ${JSON.stringify(range)}`,
    );
  const lastByte = Number(match[2]);
  if (!Number.isSafeInteger(lastByte))
    throw new Error(
      `Cloudflare registry returned an invalid upload offset: ${JSON.stringify(range)}`,
    );
  // Distribution registries commonly report 0-0 for a new, empty upload.
  return {
    location: nextLocation,
    offset: attemptedOffset === 0 && lastByte === 0 ? 0 : lastByte + 1,
  };
}

/** Uploads one OCI blob in bounded PATCH requests that can resume after a dropped TLS connection. */
export async function uploadBlobInChunks(
  repository: string,
  descriptor: OciDescriptor,
  blobPath: string,
  credentials: RegistryCredentials,
  fetcher: typeof fetch = fetch,
  chunkBytes = registryChunkBytes,
  wait: (milliseconds: number) => Promise<void> = waitMilliseconds,
): Promise<void> {
  if (
    !/^[a-z0-9._/-]+$/.test(repository) ||
    repository.startsWith("/") ||
    repository.includes("..")
  ) {
    throw new Error("Invalid Cloudflare registry repository");
  }
  if (
    !/^sha256:[a-f0-9]{64}$/.test(descriptor.digest) ||
    !Number.isSafeInteger(descriptor.size) ||
    descriptor.size < 0
  ) {
    throw new Error("Invalid OCI blob descriptor");
  }
  if (!Number.isSafeInteger(chunkBytes) || chunkBytes <= 0)
    throw new Error("Registry chunk size must be positive");

  const blobUrl = new URL(
    `/v2/${repository}/blobs/${descriptor.digest}`,
    registryOrigin,
  );
  const existing = await retryRegistryRequest(
    "Cloudflare registry blob check",
    blobUrl,
    credentials,
    { method: "HEAD" },
    fetcher,
    wait,
  );
  if (existing.status === 200) return;
  if (existing.status !== 404)
    throw new Error(
      `Cloudflare registry blob check returned HTTP ${existing.status}`,
    );

  const startUrl = new URL(`/v2/${repository}/blobs/uploads/`, registryOrigin);
  const started = await retryRegistryRequest(
    "Cloudflare registry upload start",
    startUrl,
    credentials,
    { method: "POST" },
    fetcher,
    wait,
  );
  if (started.status !== 202)
    throw new Error(
      `Cloudflare registry upload start returned HTTP ${started.status}`,
    );
  let location = uploadLocation(startUrl, started);
  let offset = 0;
  let failures = 0;
  const blob = await open(blobPath, "r");
  try {
    const file = await blob.stat();
    if (file.size !== descriptor.size)
      throw new Error(
        `OCI blob ${descriptor.digest} does not match its declared size`,
      );
    while (offset < descriptor.size) {
      const length = Math.min(chunkBytes, descriptor.size - offset);
      const bytes = Buffer.allocUnsafe(length);
      const read = await blob.read(bytes, 0, length, offset);
      if (read.bytesRead !== length)
        throw new Error(
          `OCI blob ${descriptor.digest} ended before its declared size`,
        );
      try {
        const patched = await registryRequest(
          location,
          credentials,
          {
            method: "PATCH",
            headers: {
              "content-type": "application/octet-stream",
              "content-range": `${offset}-${offset + length - 1}`,
            },
            body: bytes,
          },
          fetcher,
        );
        if (patched.status !== 202)
          throw new Error(
            `Cloudflare registry blob PATCH returned HTTP ${patched.status}`,
          );
        location = uploadLocation(location, patched);
        offset += length;
        failures = 0;
      } catch (error) {
        failures += 1;
        if (failures >= 5) throw error;
        const recovered = await recoverUploadOffset(
          location,
          credentials,
          offset,
          fetcher,
          wait,
        );
        if (recovered.offset < offset || recovered.offset > offset + length) {
          throw new Error(
            "Cloudflare registry returned an inconsistent upload offset",
          );
        }
        location = recovered.location;
        offset = recovered.offset;
        process.stderr.write(
          `Registry connection dropped; resuming ${descriptor.digest.slice(0, 19)} at byte ${offset}.\n`,
        );
      }
    }
  } finally {
    await blob.close();
  }

  const commitUrl = new URL(location);
  commitUrl.searchParams.set("digest", descriptor.digest);
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const committed = await registryRequest(
        commitUrl,
        credentials,
        { method: "PUT" },
        fetcher,
      );
      if (committed.status === 201) return;
      throw new Error(
        `Cloudflare registry blob commit returned HTTP ${committed.status}`,
      );
    } catch (error) {
      const visible = await retryRegistryRequest(
        "Cloudflare registry commit check",
        blobUrl,
        credentials,
        { method: "HEAD" },
        fetcher,
        wait,
      );
      if (visible.status === 200) return;
      if (attempt === 3) throw error;
      await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
    }
  }
}

/** Pre-stages every filesystem layer with resumable requests so crane only finalizes image metadata. */
async function stageLargeBlobs(
  layout: string,
  image: string,
  configPath: string,
): Promise<void> {
  if (!image.startsWith("registry.cloudflare.com/"))
    throw new Error("Deployment image must use the Cloudflare registry");
  const untagged = image
    .slice("registry.cloudflare.com/".length)
    .replace(/:[^/]+$/, "");
  const index = JSON.parse(
    await readFile(path.join(layout, "index.json"), "utf8"),
  ) as { manifests?: OciDescriptor[] };
  const manifestDescriptor = index.manifests?.[0];
  if (
    !manifestDescriptor ||
    !/^sha256:[a-f0-9]{64}$/.test(manifestDescriptor.digest)
  )
    throw new Error("OCI layout has no valid image manifest");
  const manifestPath = path.join(
    layout,
    "blobs",
    "sha256",
    manifestDescriptor.digest.slice("sha256:".length),
  );
  const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as {
    layers?: OciDescriptor[];
  };
  if (!Array.isArray(manifest.layers))
    throw new Error("OCI image manifest has no layers");
  const credentials = await registryCredentials(configPath);
  for (const descriptor of manifest.layers) {
    const blobPath = path.join(
      layout,
      "blobs",
      "sha256",
      descriptor.digest.slice("sha256:".length),
    );
    await uploadBlobInChunks(untagged, descriptor, blobPath, credentials);
    process.stdout.write(
      `Staged blob ${descriptor.digest} (${Math.ceil(descriptor.size / 1024 / 1024)} MiB).\n`,
    );
  }
}

/** Writes a temporary sibling config that tells Wrangler to deploy the pre-pushed image. */
export async function prebuiltImageConfig(
  configPath: string,
  image: string,
): Promise<string> {
  const value = JSON.parse(await readFile(configPath, "utf8")) as Record<
    string,
    unknown
  >;
  if (!Array.isArray(value.containers) || value.containers.length === 0)
    throw new Error("Generated Wrangler config has no container declaration");
  const containers = value.containers.map((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item))
      throw new Error(
        "Generated Wrangler config has an invalid container declaration",
      );
    return { ...item, image };
  });
  const temporary = path.join(
    path.dirname(configPath),
    `wrangler.deploy-${randomUUID()}.jsonc`,
  );
  await writeFile(
    temporary,
    `${JSON.stringify({ ...value, containers }, null, 2)}\n`,
  );
  return temporary;
}

/** Builds an OCI layout, pre-stages large layers, and finalizes the upload with crane. */
async function pushWithCrane(
  root: string,
  image: string,
  configPath: string,
): Promise<void> {
  const layout = await mkdtemp(path.join(tmpdir(), "flow-oci-"));
  try {
    const buildCode = await run("docker", [
      "buildx",
      "build",
      "--platform",
      "linux/amd64",
      "--output",
      `type=oci,tar=false,dest=${layout}`,
      root,
    ]);
    if (buildCode !== 0)
      throw new Error(`docker buildx exited with ${buildCode}`);
    await stageLargeBlobs(layout, image, configPath);
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        await loginCrane(configPath);
        const pushCode = await run("crane", ["push", layout, image]);
        if (pushCode === 0) return;
        throw new Error(`crane push exited with ${pushCode}`);
      } catch (error) {
        if (attempt === 3) {
          const detail = error instanceof Error ? error.message : String(error);
          throw new Error(
            `crane upload failed after ${attempt} attempts: ${detail}`,
            { cause: error },
          );
        }
        process.stderr.write(
          `crane upload attempt ${attempt} failed; refreshing credentials and retrying.\n`,
        );
        await waitMilliseconds(attempt * 1000);
      }
    }
  } finally {
    await rm(layout, { recursive: true, force: true });
  }
}

/** Pushes the container with crane, refreshing registry auth through Wrangler when necessary, then deploys the Worker. */
async function main(): Promise<void> {
  const root = process.cwd();
  const activeProfile = await readActiveEnvironmentProfile(root);
  assertProductionDeploymentProfile(activeProfile);
  await validateEnvironmentProfile(root, activeProfile);
  await capture("docker", ["version"]);
  await capture("crane", ["version"]);
  await capture("tar", ["--version"]);
  await generate(root, activeProfile);
  const [config, environments] = await Promise.all([
    loadWorkflowConfig(root),
    loadConfigurationEnvironments(root, activeProfile),
  ]);
  if (environments.cloudflare.CLOUDFLARE_API_TOKEN?.trim())
    process.env.CLOUDFLARE_API_TOKEN =
      environments.cloudflare.CLOUDFLARE_API_TOKEN.trim();
  const target = resolveDeploymentTarget(config, activeProfile);
  const runtime = toRuntimeConfig(
    config,
    environments.cloudflare,
    activeProfile,
  );
  if (!runtime.cloudflareAccountId)
    throw new Error("CLOUDFLARE_ACCOUNT_ID is required for deployment");

  await buildWorkflowBundle(config, root);
  const configPath = path.join(root, ".generated/wrangler.jsonc");
  const repository = `registry.cloudflare.com/${runtime.cloudflareAccountId}/${runtime.deploymentName}-agentcontainer`;
  const image = `${repository}:crane-upload`;
  await pushWithCrane(root, image, configPath);
  const workflowBundle = await syncWorkflowBundle(
    config,
    target,
    runtime.cloudflareAccountId,
    root,
    false,
  );
  const deployConfigPath = await prebuiltImageConfig(configPath, image);
  try {
    const deployCode = await runWrangler(deployConfigPath);
    if (deployCode !== 0)
      throw new Error(`wrangler deploy exited with ${deployCode}`);
    await activateWorkflowBundle(
      target,
      workflowBundle,
      runtime.cloudflareAccountId,
    );
    const apiToken = environments.secrets[config.api.auth_secret]?.trim();
    if (!runtime.runnerUrl || !apiToken)
      throw new Error(
        "RUNNER_URL and the configured API secret are required to reconcile workflow budgets after deployment",
      );
    await reconcileWorkflowBudget(runtime.runnerUrl, apiToken);
  } finally {
    await rm(deployConfigPath, { force: true });
  }
}

if (
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
