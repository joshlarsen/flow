import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadConfigurationEnvironments,
  loadOptionalWorkflow,
  loadRunnerConfig,
  requiredSecretNames,
  resolveRuntimeEnvironment,
  toRuntimeConfig,
  undeclaredRuntimeEnvironmentNames,
  validateWorkflowFiles,
} from "./config.ts";
import type { EnvironmentProfile } from "./environment-files.ts";

/** Generates Wrangler configuration and the TypeScript runtime definition from the authored configuration. */
export async function generate(
  root = process.cwd(),
  profile?: EnvironmentProfile,
): Promise<void> {
  const [config, environments] = await Promise.all([
    loadRunnerConfig(root),
    loadConfigurationEnvironments(root, profile),
  ]);
  const runtime = toRuntimeConfig(
    config,
    environments.cloudflare,
    environments.files.profile,
  );
  const workflow = await loadOptionalWorkflow(config, root);
  const workflowSchedules = workflow?.schedules ?? [];
  if (workflowSchedules.length > 0 && !runtime.runnerUrl) {
    throw new Error(
      "RUNNER_URL is required when scheduled workflows use callbacks or token budgets",
    );
  }
  const runtimeVariables = resolveRuntimeEnvironment(
    config,
    environments.runtime,
  );
  const generatedDir = path.join(root, ".generated");
  const sourceDir = path.join(root, "src");
  const requiredSecrets = requiredSecretNames(config, workflow);
  await Promise.all([
    mkdir(generatedDir, { recursive: true }),
    mkdir(sourceDir, { recursive: true }),
  ]);

  const wrangler = {
    $schema: "../node_modules/wrangler/config-schema.json",
    name: runtime.deploymentName,
    ...(runtime.cloudflareAccountId
      ? { account_id: runtime.cloudflareAccountId }
      : {}),
    main: "../src/index.ts",
    compatibility_date: config.deployment.compatibility_date,
    compatibility_flags: [
      "nodejs_compat",
      ...(config.deployment.global_fetch_strictly_public
        ? ["global_fetch_strictly_public"]
        : []),
    ],
    containers: [
      {
        class_name: "AgentContainer",
        image: "../Dockerfile",
        max_instances: config.container.max_instances,
        instance_type: config.container.instance_type,
      },
    ],
    durable_objects: {
      bindings: [
        { name: "AGENT_CONTAINER", class_name: "AgentContainer" },
        { name: "JOB_COORDINATOR", class_name: "JobCoordinator" },
        { name: "SCHEDULE_COORDINATOR", class_name: "ScheduleCoordinator" },
        { name: "WORKFLOW_BUDGET", class_name: "WorkflowBudgetCoordinator" },
        {
          name: "OAUTH_CREDENTIAL_BROKER",
          class_name: "OAuthCredentialBroker",
        },
      ],
    },
    migrations: [
      {
        tag: "v1",
        new_sqlite_classes: [
          "AgentContainer",
          "JobCoordinator",
          "OAuthCredentialBroker",
          "ScheduleCoordinator",
          "WorkflowBudgetCoordinator",
        ],
      },
    ],
    observability: {
      enabled: true,
      traces: {
        enabled: config.observability.traces.enabled,
        persist: config.observability.traces.persist,
        head_sampling_rate: config.observability.traces.head_sampling_rate,
      },
    },
    r2_buckets: [
      {
        binding: "RUNNER_STORAGE",
        bucket_name: runtime.storageBucketName,
        remote: true,
      },
    ],
    vars: runtimeVariables,
    triggers: { crons: workflowSchedules.map((schedule) => schedule.cron) },
    secrets: { required: requiredSecrets },
  };

  const generatedTs = `// Generated from config/*.yaml. Do not edit.\nexport const appConfig = ${JSON.stringify({ ...runtime, workflowSchedules }, null, 2)} as const as ReturnType<typeof import("../tools/config.ts").toRuntimeConfig> & { readonly workflowSchedules: readonly unknown[] };\n`;
  await Promise.all([
    writeFile(
      path.join(generatedDir, "wrangler.jsonc"),
      `${JSON.stringify(wrangler, null, 2)}\n`,
    ),
    writeFile(path.join(sourceDir, "generated-config.ts"), generatedTs),
  ]);
}

/** Validates configuration and optionally refreshes generated artifacts. */
async function main() {
  const root = process.cwd();
  const [config, environments] = await Promise.all([
    loadRunnerConfig(root),
    loadConfigurationEnvironments(root),
  ]);
  const hasActiveWorkflow = process.argv.includes("--check")
    ? await validateWorkflowFiles(config, root)
    : false;
  toRuntimeConfig(config, environments.cloudflare, environments.files.profile);
  resolveRuntimeEnvironment(config, environments.runtime);
  if (process.argv.includes("--check")) {
    const undeclared = undeclaredRuntimeEnvironmentNames(
      config,
      environments.local.runtime,
    );
    if (undeclared.length > 0) {
      process.stderr.write(
        `WARNING .env contains variables not declared in config/catalog.yaml: ${undeclared.join(", ")}\n`,
      );
    }
  }
  if (!process.argv.includes("--check")) await generate();
  process.stdout.write(
    `Configuration is valid${process.argv.includes("--check") ? (hasActiveWorkflow ? "; active workflow is valid" : "; no active workflow configured") : "; generated configuration updated"}\n`,
  );
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
