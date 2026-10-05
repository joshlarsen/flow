import { randomUUID } from "node:crypto";
import { appendFile, chmod, readFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "dotenv";
import {
  basicAuthValuePrefix,
  loadConfigurationEnvironments,
  loadWorkflowConfig,
  requiredSecretNames,
  type RunnerConfig,
  toRuntimeConfig,
} from "./config.ts";
import {
  readActiveEnvironmentProfile,
  requireEnvironmentProfile,
  type EnvironmentProfile,
} from "./environment-files.ts";
import { validateEnvironmentProfile } from "./environment.ts";
import { buildWorkflowBundle } from "./workflow-bundle.ts";
import { generate } from "./generate-config.ts";

/** Prevents local credentials from mutating the deployed production Worker. */
export function assertProductionSecretSyncProfile(
  profile: EnvironmentProfile | null,
): asserts profile is "prod" {
  requireEnvironmentProfile(profile, "prod", "pnpm secrets:sync");
}

/** Rejects malformed raw Basic credentials before any remote synchronization. */
export function validateBasicCredentialSecrets(
  credentials: RunnerConfig["credentials"],
  values: Record<string, string | undefined>,
): void {
  for (const [credentialName, credential] of Object.entries(credentials)) {
    if (!("secret" in credential.upstream)) continue;
    if (credential.upstream.value_prefix !== basicAuthValuePrefix) continue;
    const value = values[credential.upstream.secret];
    if (value === undefined || value === "") continue;
    if (!value.includes(":") || /[\u0000-\u001f\u007f]/u.test(value)) {
      throw new Error(
        `Credential ${credentialName} secret ${credential.upstream.secret} must be a valid Basic username:password value`,
      );
    }
  }
}

/** Validates local secrets and synchronizes YAML-referenced values to Cloudflare. */
async function main() {
  const root = process.cwd();
  const activeProfile = await readActiveEnvironmentProfile(root);
  assertProductionSecretSyncProfile(activeProfile);
  await generate(root, activeProfile);
  const [config, environments] = await Promise.all([
    loadWorkflowConfig(root),
    loadConfigurationEnvironments(root, activeProfile),
  ]);
  const envPath = environments.files.secrets;
  let envFile = "";
  try {
    envFile = await readFile(envPath, "utf8");
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
  const values = { ...parse(envFile), ...process.env };
  const required = requiredSecretNames(config);
  const payload: Record<string, string> = {};

  if (!values[config.api.auth_secret]) {
    const separator = envFile.length > 0 && !envFile.endsWith("\n") ? "\n" : "";
    const generated = randomUUID().replaceAll("-", "");
    values[config.api.auth_secret] = generated;
    await appendFile(
      envPath,
      `${separator}${config.api.auth_secret}=${generated}\n`,
      { mode: 0o600 },
    );
    await chmod(envPath, 0o600);
    process.stdout.write(
      `Generated ${config.api.auth_secret} in ${envPath}.\n`,
    );
  }

  for (const name of required) {
    const value = values[name];
    if (!value)
      throw new Error(`Missing required secret ${name} in ${envPath}`);
    if (name === config.api.auth_secret && value.length < 32) {
      throw new Error(`${name} must be at least 32 characters`);
    }
    payload[name] = value;
  }
  validateBasicCredentialSecrets(config.credentials, payload);

  if (environments.cloudflare.CLOUDFLARE_API_TOKEN?.trim())
    process.env.CLOUDFLARE_API_TOKEN =
      environments.cloudflare.CLOUDFLARE_API_TOKEN.trim();
  await validateEnvironmentProfile(root, activeProfile);
  await buildWorkflowBundle(config, root);
  await new Promise<void>((resolve, reject) => {
    const child = spawn(
      "pnpm",
      [
        "wrangler",
        "--cwd",
        tmpdir(),
        "secret",
        "bulk",
        "-c",
        path.join(root, ".generated/wrangler.jsonc"),
      ],
      {
        cwd: root,
        stdio: ["pipe", "inherit", "inherit"],
      },
    );
    child.once("error", reject);
    child.once("exit", (code) =>
      code === 0
        ? resolve()
        : reject(new Error(`wrangler secret bulk exited with ${code}`)),
    );
    child.stdin.end(JSON.stringify(payload));
  });

  process.stdout.write(`Synchronized ${required.length} declared secrets.\n`);
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
