#!/usr/bin/env node

import {
  loadConfigurationEnvironments,
  loadWorkflowConfig,
  resolveDeploymentTarget,
  toRuntimeConfig,
} from "./config.ts";
import {
  readActiveEnvironmentProfile,
  requireActiveEnvironmentProfile,
} from "./environment-files.ts";
import {
  reconcileWorkflowBudget,
  syncWorkflowBundle,
} from "./workflow-bundle.ts";

async function main(): Promise<void> {
  const root = process.cwd();
  const profile = requireActiveEnvironmentProfile(
    await readActiveEnvironmentProfile(root),
    "pnpm bundle:sync",
  );
  const [config, environments] = await Promise.all([
    loadWorkflowConfig(root),
    loadConfigurationEnvironments(root, profile),
  ]);
  if (environments.cloudflare.CLOUDFLARE_API_TOKEN?.trim())
    process.env.CLOUDFLARE_API_TOKEN =
      environments.cloudflare.CLOUDFLARE_API_TOKEN.trim();
  const target = resolveDeploymentTarget(config, profile);
  const runtime = toRuntimeConfig(config, environments.cloudflare, profile);
  const accountId = runtime.cloudflareAccountId;
  if (!accountId)
    throw new Error(
      "CLOUDFLARE_ACCOUNT_ID is required to synchronize the workflow bundle",
    );
  const apiToken = environments.secrets[config.api.auth_secret]?.trim();
  if (config.workflow.token_budget && (!runtime.runnerUrl || !apiToken)) {
    throw new Error(
      "RUNNER_URL and the configured API secret are required to activate a token-budgeted workflow",
    );
  }
  process.stdout.write(
    `Synchronizing ${profile} workflow bundle to ${target.storageBucketName}.\n`,
  );
  const manifest = await syncWorkflowBundle(config, target, accountId, root);
  if (runtime.runnerUrl && apiToken)
    await reconcileWorkflowBudget(runtime.runnerUrl, apiToken);
  process.stdout.write(
    `Activated ${profile} workflow ${manifest.workflow.name} bundle ${manifest.digest}.\n`,
  );
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
