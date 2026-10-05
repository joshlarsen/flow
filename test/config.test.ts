import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { stringify } from "yaml";
import { describe, expect, it } from "vitest";
import {
  durationToMilliseconds,
  environmentDeploymentName,
  isCloudflareCronExpression,
  loadConfigurationEnvironments,
  loadRunnerConfig,
  loadWorkflowConfig,
  requiredSecretNames,
  resolveDeploymentTarget,
  storageBucketName,
  toRuntimeConfig,
  validateWorkflowFiles,
  runnerSettingsSchema,
  resolveRuntimeEnvironment,
  undeclaredRuntimeEnvironmentNames,
} from "../tools/config.ts";
import { generate } from "../tools/generate-config.ts";

const runner = {
  version: 1,
  deployment: { name: "runner", compatibility_date: "2026-08-29" },
  container: { instance_type: "standard-2", max_instances: 5, port: 8080 },
  api: { auth_secret: "RUNNER_API_TOKEN", retention: "24h" },
  history: { max_verbose_bytes: 26214400, max_metric_bytes: 5242880, max_lifecycle_bytes: 1048576, max_pending_bytes: 8388608 },
  runner: { shutdown_grace: "10s", max_prompt_bytes: 65536, max_prompt_files: 100, max_prompt_bundle_bytes: 1048576, max_asset_bytes: 5242880, max_asset_files: 500, max_asset_bundle_bytes: 26214400, max_result_bytes: 1048576 },
  artifacts: { max_files: 50, max_file_bytes: 5242880, max_total_bytes: 26214400 },
  memory: { max_database_bytes: 26214400, persistence_timeout: "2m" },
  interactions: { provider: "none" },
  observability: { traces: { enabled: true, persist: true, head_sampling_rate: 1 } },
  logging: { events: "full", max_log_bytes: 240000 },
};
const catalog = {
  version: 1,
  credentials: { token: { source: { header: "Authorization", value_prefix: "Bearer " }, upstream: { header: "Authorization", secret: "OPENAI_API_KEY", value_prefix: "Bearer " } } },
  providers: { azure: { protocol: "openai-responses", endpoint: { kind: "url", base_url: "https://example.openai.azure.com/openai/v1" }, credential: "token", static_headers: {} } },
  models: { "gpt-5.6-terra": { providers: { azure: "gpt-5.6-terra" } } },
  harnesses: { codex: { type: "codex" } }, routes: {},
};
const workflow = {
  version: 1, name: "default", memory: false, workflow_timeout: "1h",
  defaults: { step_timeout: "15m", harness: "codex", provider: "azure", model: "gpt-5.6-terra", reasoning_effort: "medium" },
  steps: [{ id: "run", prompt: "run.md" }],
};
const command = { id: "check", command: ["curl", "--fail", "https://example.com"], timeout: "30s" };

async function project(overrides: { runner?: unknown; catalog?: unknown; workflow?: unknown | null; example?: unknown | null } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), "runner-config-"));
  const configRoot = path.join(root, "config");
  await mkdir(configRoot);
  await mkdir(path.join(root,".env.profiles/unselected"),{recursive:true});
  await Promise.all([
    writeFile(path.join(configRoot, "runner.yaml"), stringify(overrides.runner ?? runner)),
    writeFile(path.join(configRoot, "catalog.yaml"), stringify(overrides.catalog ?? catalog)),
    ...(overrides.workflow === null ? [] : [writeFile(path.join(configRoot, "workflow.yaml"), stringify(overrides.workflow ?? workflow))]),
    ...(overrides.example === null ? [] : [writeFile(path.join(configRoot, "workflow.yaml.example"), stringify(overrides.example ?? workflow))]),
  ]);
  return root;
}

describe("runner configuration", () => {
  it("requires only active providers and configured credential routes",async()=>{
    const extra={source:{header:"Authorization",value_prefix:"Bearer "},upstream:{header:"Authorization",secret:"UNUSED_KEY",value_prefix:"Bearer "}};
    const config=await loadWorkflowConfig(await project({catalog:{...catalog,credentials:{...catalog.credentials,unused:extra},providers:{...catalog.providers,unused:{...catalog.providers.azure,credential:"unused"}}}}));
    expect(requiredSecretNames(config)).toEqual(["OPENAI_API_KEY","RUNNER_API_TOKEN"]);
    config.workflow.steps=[{id:"check",command:["true"]}];expect(requiredSecretNames(config)).toEqual(["RUNNER_API_TOKEN"]);
    config.routes.unused={credential:"unused",url_prefix:"https://api.example.com"};expect(requiredSecretNames(config)).toEqual(["RUNNER_API_TOKEN","UNUSED_KEY"]);
  });

  it("keeps workflow timeout policy out of runner.yaml", () => {
    expect(() => runnerSettingsSchema.parse({
      ...runner,
      runner: { ...runner.runner, workflow_timeout: "1h", default_step_timeout: "15m" },
    })).toThrow();
  });

  it("normalizes Slack interaction policy without exposing its secrets to the container", async () => {
    const slack = {
      ...runner,
      deployment: { ...runner.deployment, global_fetch_strictly_public: true },
      interactions: {
        provider: "slack", live_wait: "30s", response_ttl: "24h",
        max_request_bytes: 65536, max_response_bytes: 65536,
        checkpoint: { max_files: 1000, max_file_bytes: 1048576, max_total_bytes: 8388608 },
        team_id: "T123", conversation_id: "C456", allowed_user_ids: ["U789"],
        bot_token_secret: "SLACK_BOT_TOKEN", signing_secret: "SLACK_SIGNING_SECRET",
      },
    };
    const config = await loadRunnerConfig(await project({ runner: slack }));
    const runtime = toRuntimeConfig(config);
    expect(runtime.interactions).toMatchObject({ provider: "slack", liveWaitMs: 30_000, responseTtlMs: 86_400_000 });
    expect(runtime.runnerConfig.interactions).toMatchObject({ provider: "callback", live_wait_timeout_ms: 30_000 });
    expect(JSON.stringify(runtime.runnerConfig)).not.toContain("SLACK_BOT_TOKEN");
    expect(requiredSecretNames(config,workflow as any)).toEqual(expect.arrayContaining(["SLACK_BOT_TOKEN", "SLACK_SIGNING_SECRET"]));
  });

  it("requires a single container for persistent workflow memory", async () => {
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, memory: true } }))).rejects.toThrow(/max_instances.*equal 1/);
    const single = { ...runner, container: { ...runner.container, max_instances: 1 } };
    await expect(loadWorkflowConfig(await project({ runner: single, workflow: { ...workflow, memory: true } }))).resolves.toMatchObject({
      workflow: { memory: true },
    });
  });

  it("defaults workflow memory off and validates its runner limits", async () => {
    const { memory: _memory, ...withoutMemory } = workflow;
    await expect(loadWorkflowConfig(await project({ workflow: withoutMemory }))).resolves.toMatchObject({ workflow: { memory: false } });
    expect(() => runnerSettingsSchema.parse({
      ...runner,
      memory: { ...runner.memory, max_database_bytes: 4095 },
    })).toThrow();
  });

  it("accepts only bounded workflow-level recurring token budgets", async () => {
    await expect(loadWorkflowConfig(await project({ workflow: {
      ...workflow, token_budget: { limit: "1_000_000", period: "week" },
    } }))).resolves.toMatchObject({ workflow: { token_budget: { limit: 1_000_000, period: "week" } } });
    await expect(loadWorkflowConfig(await project({ workflow: {
      ...workflow, token_budget: { limit: 0, period: "day" },
    } }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: {
      ...workflow, token_budget: { limit: 100, period: "month" },
    } }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: {
      ...workflow, token_budget: { limit: "1_00_000", period: "day" },
    } }))).rejects.toThrow();
  });

  it("loads split configuration and derives runtime values", async () => {
    const config = await loadRunnerConfig(await project());
    const runtime = toRuntimeConfig(config);
    expect(durationToMilliseconds("1h")).toBe(3600000);
    expect(runtime.runnerConfig).not.toHaveProperty("workflow_timeout_ms");
    expect(runtime.runnerConfig).not.toHaveProperty("default_step_timeout_ms");
    expect(runtime.memoryLimits).toEqual({ maxDatabaseBytes: 26214400, persistenceTimeoutMs: 120000 });
    expect(runtime.runnerConfig).toEqual(expect.objectContaining({ max_memory_bytes: 26214400, memory_persistence_timeout_ms: 120000 }));
    expect(runtime.runnerConfig.models["gpt-5.6-terra"]!.azure).toBe("gpt-5.6-terra");
    expect(runtime.credentialInjections[0]).toEqual(expect.objectContaining({ targetKind: "provider", targetName: "azure" }));
    expect(runtime.credentialInjections[0]!.upstream).toEqual({
      kind: "static", header: "authorization", secret: "OPENAI_API_KEY", valuePrefix: "Bearer ",
    });
  });

  it("validates canonical Cloudflare Cron expressions locally", () => {
    for (const cron of ["* * * * *", "*/30 * * * *", "0 17 * * sun", "10 7 * * MON-FRI", "0 18 * * 6L", "59 23 LW * *", "0 9 15W JAN MON#2"]) {
      expect(isCloudflareCronExpression(cron), cron).toBe(true);
    }
    for (const cron of ["0 0 0 * * *", "0  0 * * *", "60 * * * *", "0 0 * * 0", "0 0 ? * *", "0 0 * DEC-JAN *", "0 0 0W * *"]) {
      expect(isCloudflareCronExpression(cron), cron).toBe(false);
    }
  });

  it("requires uniquely named schedules with uniquely routable expressions", async () => {
    const scheduled = { ...workflow, schedules: [
      { id: "morning", cron: "0 13 * * MON-FRI" },
      { id: "evening", cron: "0 22 * * MON-FRI" },
    ] };
    await expect(loadWorkflowConfig(await project({ workflow: scheduled }))).resolves.toMatchObject({ workflow: { schedules: scheduled.schedules } });
    await expect(loadWorkflowConfig(await project({ workflow: { ...scheduled, schedules: [...scheduled.schedules, { id: "morning", cron: "0 1 * * *" }] } }))).rejects.toThrow(/unique/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...scheduled, schedules: [...scheduled.schedules, { id: "other", cron: scheduled.schedules[0]!.cron }] } }))).rejects.toThrow(/Cloudflare identifies triggers/);
  });

  it("loads base configuration without an active workflow", async () => {
    const root = await project({ workflow: null });
    await expect(loadRunnerConfig(root)).resolves.toBeDefined();
    await expect(loadWorkflowConfig(root)).rejects.toThrow(/copy config\/workflow.yaml.example/);
    await expect(validateWorkflowFiles(await loadRunnerConfig(root), root)).resolves.toBe(false);
  });


  it("retains the Basic prefix used by credential injection rules", async () => {
    const configuredCatalog = structuredClone(catalog);
    configuredCatalog.credentials.token.source.value_prefix = "Basic ";
    configuredCatalog.credentials.token.upstream.value_prefix = "Basic ";
    const runtime = toRuntimeConfig(await loadRunnerConfig(await project({ catalog: configuredCatalog })));
    expect(runtime.credentialInjections[0]!.sourceValuePrefix).toBe("Basic ");
    expect(runtime.credentialInjections[0]!.upstream).toEqual(expect.objectContaining({ valuePrefix: "Basic " }));
  });

  it("requires canonical Basic prefixes on Authorization headers", async () => {
    const wrongHeader = structuredClone(catalog);
    wrongHeader.credentials.token.source = {
      ...wrongHeader.credentials.token.source,
      header: "X-Api-Key",
      value_prefix: "Basic ",
    };
    await expect(loadRunnerConfig(await project({ catalog: wrongHeader }))).rejects.toThrow(/Authorization header/);

    const wrongPrefix = structuredClone(catalog);
    wrongPrefix.credentials.token.upstream.value_prefix = "basic ";
    await expect(loadRunnerConfig(await project({ catalog: wrongPrefix }))).rejects.toThrow(/exact value_prefix/);
  });

  it("normalizes OAuth credentials and discovers every referenced Worker Secret", async () => {
    const configuredCatalog: any = structuredClone(catalog);
    configuredCatalog.credentials.token.upstream = {
      oauth: {
        token_url: "https://auth.example.com/oauth/token",
        client_id_secret: "OAUTH_CLIENT_ID",
        client_auth: { method: "client_secret_post", secret: "OAUTH_CLIENT_SECRET" },
        grant: { type: "refresh_token", refresh_token_secret: "OAUTH_REFRESH_TOKEN" },
        scopes: ["read", "write"],
        extra_parameters: { audience: "https://api.example.com" },
        fallback_ttl: "10m",
      },
    };
    const config = await loadRunnerConfig(await project({ catalog: configuredCatalog }));
    const runtime = toRuntimeConfig(config);

    expect(runtime.credentialInjections[0]!.upstream).toEqual({ kind: "oauth" });
    expect(runtime.oauthCredentials.token).toEqual({
      tokenUrl: "https://auth.example.com/oauth/token",
      clientIdSecret: "OAUTH_CLIENT_ID",
      clientAuth: { method: "client_secret_post", secret: "OAUTH_CLIENT_SECRET" },
      grant: { type: "refresh_token", refreshTokenSecret: "OAUTH_REFRESH_TOKEN" },
      scopes: ["read", "write"],
      extraParameters: { audience: "https://api.example.com" },
      fallbackTtlMs: 600_000,
    });
    expect(requiredSecretNames(config,workflow as any)).toEqual(expect.arrayContaining([
      "OAUTH_CLIENT_ID", "OAUTH_CLIENT_SECRET", "OAUTH_REFRESH_TOKEN",
    ]));
  });

  it("rejects unsafe or contradictory OAuth credential configuration", async () => {
    const publicClientCredentials: any = structuredClone(catalog);
    publicClientCredentials.credentials.token.upstream = {
      oauth: {
        token_url: "https://auth.example.com/token",
        client_id_secret: "OAUTH_CLIENT_ID",
        client_auth: { method: "none" },
        grant: { type: "client_credentials" },
      },
    };
    await expect(loadRunnerConfig(await project({ catalog: publicClientCredentials }))).rejects.toThrow(/confidential client authentication/);

    const reservedParameter: any = structuredClone(catalog);
    reservedParameter.credentials.token.upstream = {
      oauth: {
        token_url: "https://auth.example.com/token",
        client_id_secret: "OAUTH_CLIENT_ID",
        client_auth: { method: "client_secret_basic", secret: "OAUTH_CLIENT_SECRET" },
        grant: { type: "client_credentials" },
        extra_parameters: { grant_type: "password" },
      },
    };
    await expect(loadRunnerConfig(await project({ catalog: reservedParameter }))).rejects.toThrow(/managed by the OAuth credential configuration/);

    const unsafeUrl: any = structuredClone(reservedParameter);
    unsafeUrl.credentials.token.upstream.oauth.extra_parameters = {};
    unsafeUrl.credentials.token.upstream.oauth.token_url = "https://auth.example.com/token?secret=value";
    await expect(loadRunnerConfig(await project({ catalog: unsafeUrl }))).rejects.toThrow(/query or fragment/);
  });


  it("rejects plaintext environment names that overlap secrets or credential aliases", async () => {
    const secretCollision = { ...structuredClone(catalog), environment: { OPENAI_API_KEY: { type: "plaintext" } } };
    await expect(loadRunnerConfig(await project({ catalog: secretCollision }))).rejects.toThrow(/also declared as a Worker Secret/);
    const aliasCollision = structuredClone(catalog);
    aliasCollision.credentials.token.source = { ...aliasCollision.credentials.token.source, env: "SERVICE_URL" } as typeof aliasCollision.credentials.token.source & { env: string };
    Object.assign(aliasCollision, { environment: { SERVICE_URL: { type: "plaintext" } } });
    await expect(loadRunnerConfig(await project({ catalog: aliasCollision }))).rejects.toThrow(/conflicts with a plaintext/);
  });

  it("loads separate environment domains and rejects duplicate local definitions", async () => {
    const root = await project();
    await Promise.all([
      writeFile(path.join(root, ".env"), "SERVICE_URL=https://service.example\n"),
      writeFile(path.join(root, ".env.profiles/unselected/.env.secrets"), "LOCAL_SECRET_VALUE=secret\n"),
      writeFile(path.join(root, ".env.profiles/unselected/.env.cloudflare"), "LOCAL_INFRA_VALUE=infra\n"),
    ]);
    const environments = await loadConfigurationEnvironments(root);
    expect(environments.runtime.SERVICE_URL).toBe("https://service.example");
    expect(environments.secrets.LOCAL_SECRET_VALUE).toBe("secret");
    expect(environments.cloudflare.LOCAL_INFRA_VALUE).toBe("infra");
    await writeFile(path.join(root, ".env.profiles/unselected/.env.cloudflare"), "SERVICE_URL=https://wrong.example\n");
    await expect(loadConfigurationEnvironments(root)).rejects.toThrow(/defined in both .env and .*env.cloudflare/);
  });

  it("loads the active profile while keeping runtime values shared", async () => {
    const root = await project();
    const profile = path.join(root, ".env.profiles", "prod");
    await mkdir(profile, { recursive: true });
    await Promise.all([
      writeFile(path.join(root, ".env"), "SHARED_VALUE=shared\n"),
      writeFile(path.join(profile, ".env.secrets"), "PROFILE_SECRET=prod-secret\n"),
      writeFile(path.join(profile, ".env.cloudflare"), "PROFILE_SETTING=prod-setting\n"),
      writeFile(path.join(root, ".env.active"), "prod\n"),
    ]);
    const environments = await loadConfigurationEnvironments(root);
    expect(environments.files.profile).toBe("prod");
    expect(environments.runtime.SHARED_VALUE).toBe("shared");
    expect(environments.secrets.PROFILE_SECRET).toBe("prod-secret");
    expect(environments.cloudflare.PROFILE_SETTING).toBe("prod-setting");
  });

  it("rejects an incomplete active profile", async () => {
    const root = await project();
    await mkdir(path.join(root, ".env.profiles", "local"), { recursive: true });
    await writeFile(path.join(root, ".env.active"), "local\n");
    await expect(loadConfigurationEnvironments(root)).rejects.toThrow(/Missing environment profile file/);
  });

  it("emits plaintext Worker vars while generated runtime artifacts contain names only", async () => {
    const configuredCatalog = { ...structuredClone(catalog), environment: { SERVICE_URL: { type: "plaintext" } } };
    const root = await project({ catalog: configuredCatalog });
    await writeFile(path.join(root, ".env"), "SERVICE_URL=https://service.example\n");
    await generate(root);
    const wrangler = JSON.parse(await readFile(path.join(root, ".generated", "wrangler.jsonc"), "utf8"));
    const generatedTypeScript = await readFile(path.join(root, "src", "generated-config.ts"), "utf8");
    const generatedRunner = toRuntimeConfig(await loadRunnerConfig(root)).runnerConfig;
    expect(wrangler.vars).toEqual({ SERVICE_URL: "https://service.example" });
    expect(wrangler.compatibility_flags).toEqual(["nodejs_compat"]);
    expect(wrangler.observability).toEqual({
      enabled: true,
      traces: { enabled: true, persist: true, head_sampling_rate: 1 },
    });
    expect(wrangler.durable_objects.bindings).toContainEqual({
      name: "OAUTH_CREDENTIAL_BROKER", class_name: "OAuthCredentialBroker",
    });
    expect(wrangler.migrations[0].new_sqlite_classes).toEqual(expect.arrayContaining(["AgentContainer","JobCoordinator","OAuthCredentialBroker","ScheduleCoordinator","WorkflowBudgetCoordinator"]));
    expect(wrangler.durable_objects.bindings).toContainEqual({
      name: "SCHEDULE_COORDINATOR", class_name: "ScheduleCoordinator",
    });
    expect(wrangler.migrations[0].new_sqlite_classes).toEqual(expect.arrayContaining(["AgentContainer","JobCoordinator","OAuthCredentialBroker","ScheduleCoordinator","WorkflowBudgetCoordinator"]));
    expect(wrangler.durable_objects.bindings).toContainEqual({
      name: "WORKFLOW_BUDGET", class_name: "WorkflowBudgetCoordinator",
    });
    expect(wrangler.migrations[0].new_sqlite_classes).toEqual(expect.arrayContaining(["AgentContainer","JobCoordinator","OAuthCredentialBroker","ScheduleCoordinator","WorkflowBudgetCoordinator"]));
    expect(wrangler.triggers).toEqual({ crons: [] });
    expect(generatedTypeScript).toContain('"runtimeEnvironment": [');
    expect(generatedTypeScript).not.toContain("https://service.example");
    expect(generatedRunner.runtime_environment).toEqual(["SERVICE_URL"]);
  });

  it("generates exact native Cron Triggers from the optional active workflow", async () => {
    const schedules = [
      { id: "weekday", cron: "0 13 * * MON-FRI" },
      { id: "month-end", cron: "59 23 LW * *" },
    ];
    const root = await project({ workflow: { ...workflow, schedules } });
    await writeFile(path.join(root,".env.profiles/unselected/.env.cloudflare"),"RUNNER_URL=https://runner.test\n");
    await generate(root);
    const wrangler = JSON.parse(await readFile(path.join(root, ".generated", "wrangler.jsonc"), "utf8"));
    const generated = await readFile(path.join(root, "src", "generated-config.ts"), "utf8");
    expect(wrangler.triggers).toEqual({ crons: schedules.map((schedule) => schedule.cron) });
    expect(generated).toContain('"workflowSchedules": [');
    expect(generated).toContain('"id": "weekday"');
  });

  it("requires a public callback origin for scheduled workflows", async () => {
    const root = await project({ workflow: { ...workflow, schedules: [{ id: "daily", cron: "0 13 * * *" }] } });
    await writeFile(path.join(root, ".env.profiles/unselected/.env.cloudflare"), "");
    await expect(generate(root)).rejects.toThrow(/RUNNER_URL is required/);
    await writeFile(path.join(root, ".env.profiles/unselected/.env.cloudflare"), "RUNNER_URL=https://runner.example.com\n");
    await expect(generate(root)).resolves.toBeUndefined();
    const generated = await readFile(path.join(root, "src", "generated-config.ts"), "utf8");
    expect(generated).toContain('"runnerUrl": "https://runner.example.com"');
  });

  it("requires persisted tracing and validates its sampling rate", () => {
    expect(() => runnerSettingsSchema.parse({
      ...runner,
      observability: { traces: { ...runner.observability.traces, enabled: false } },
    })).toThrow();
    expect(() => runnerSettingsSchema.parse({
      ...runner,
      observability: { traces: { ...runner.observability.traces, persist: false } },
    })).toThrow();
    expect(() => runnerSettingsSchema.parse({
      ...runner,
      observability: { traces: { ...runner.observability.traces, head_sampling_rate: 1.01 } },
    })).toThrow();
  });

  it("optionally routes same-zone global fetches through the public Worker", async () => {
    const root = await project({
      runner: {
        ...runner,
        deployment: {
          ...runner.deployment,
          global_fetch_strictly_public: true,
        },
      },
    });
    await generate(root);
    const wrangler = JSON.parse(await readFile(path.join(root, ".generated", "wrangler.jsonc"), "utf8"));
    expect(wrangler.compatibility_flags).toEqual(["nodejs_compat", "global_fetch_strictly_public"]);
  });

  it("rejects incompatible workflow references", async () => {
    const root = await project({ workflow: { ...workflow, steps: [{ ...workflow.steps[0], provider: "missing" }] } });
    await expect(loadWorkflowConfig(root)).rejects.toThrow(/unknown provider/);
    await expect(validateWorkflowFiles(await loadRunnerConfig(root), root)).rejects.toThrow(/unknown provider/);
  });

  it("inherits partial defaults and allows independent step overrides", async () => {
    const { provider: _provider, model: _model, ...partialDefaults } = workflow.defaults;
    const partialRoot = await project({ workflow: {
      ...workflow,
      defaults: partialDefaults,
      steps: [{ id: "run", prompt: "run.md", provider: "azure", model: "gpt-5.6-terra" }],
    } });
    await expect(loadWorkflowConfig(partialRoot)).resolves.toBeDefined();

    const extendedCatalog = {
      ...structuredClone(catalog),
      models: { ...structuredClone(catalog.models), "gpt-alt": { providers: { azure: "gpt-alt" } } },
    };
    const overrideRoot = await project({
      catalog: extendedCatalog,
      workflow: { ...workflow, steps: [{ id: "run", prompt: "run.md", model: "gpt-alt" }] },
    });
    const config = await loadWorkflowConfig(overrideRoot);
    expect(config.workflow.steps[0]).toEqual({ id: "run", prompt: "run.md", allow_user_input: false, required_metrics: [], model: "gpt-alt" });
  });

  it("normalizes grouped required metric definitions", async () => {
    const configured = await loadWorkflowConfig(await project({ workflow: {
      ...workflow,
      steps: [{ ...workflow.steps[0], required_metrics: [
        { customers: [{ free: "number of free customers" }, { pro: "number of pro customers" }] },
        { quality: [{ score: "quality score" }] },
      ] }],
    } }));
    expect(configured.workflow.steps[0]).toMatchObject({ required_metrics: [
      { namespace: "customers", key: "free", description: "number of free customers" },
      { namespace: "customers", key: "pro", description: "number of pro customers" },
      { namespace: "quality", key: "score", description: "quality score" },
    ] });
  });

  it("rejects legacy and malformed required metric definitions", async () => {
    const parse = async (required_metrics: unknown) => loadWorkflowConfig(await project({ workflow: {
      ...workflow, steps: [{ ...workflow.steps[0], required_metrics }],
    } }));
    await expect(parse(["customers.pro"])).rejects.toThrow();
    await expect(parse([{ Customers: [{ pro: "count" }] }])).rejects.toThrow();
    await expect(parse([{ customers: [{ pro: " " }] }])).rejects.toThrow();
    await expect(parse([{ customers: [{ pro: "line one\nline two" }] }])).rejects.toThrow();
    await expect(parse([{ customers: [{ pro: "first" }] }, { customers: [{ pro: "duplicate" }] }])).rejects.toThrow(/duplicate metric customers\.pro/);
    await expect(parse([{ customers: Array.from({ length: 101 }, (_, index) => ({ [`key_${index}`]: "count" })) }])).rejects.toThrow(/at most 100 metrics/);
  });

  it("inherits, overrides, and clears reasoning effort", async () => {
    const configured = await loadWorkflowConfig(await project({ workflow: {
      ...workflow,
      steps: [
        { id: "run", prompt: "run.md" },
        { id: "review", prompt: "review.md", reasoning_effort: "high" },
        { id: "native", prompt: "native.md", reasoning_effort: null },
      ],
    } }));
    expect(configured.workflow.defaults.reasoning_effort).toBe("medium");
    expect(configured.workflow.steps[1]).toEqual({ id: "review", prompt: "review.md", allow_user_input: false, required_metrics: [], reasoning_effort: "high" });
    expect(configured.workflow.steps[2]).toEqual({ id: "native", prompt: "native.md", allow_user_input: false, required_metrics: [], reasoning_effort: null });
  });

  it("accepts every canonical reasoning effort", async () => {
    for (const reasoning_effort of ["none", "minimal", "low", "medium", "high", "xhigh", "max"] as const) {
      await expect(loadWorkflowConfig(await project({ workflow: {
        ...workflow,
        defaults: { ...workflow.defaults, reasoning_effort },
      } }))).resolves.toBeDefined();
    }
  });

  it("rejects unresolved or invalid workflow defaults", async () => {
    const { harness: _harness, ...withoutHarness } = workflow.defaults;
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, defaults: withoutHarness } }))).rejects.toThrow(/must be set on the step or workflow defaults/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, defaults: { ...workflow.defaults, provider: "missing" } } }))).rejects.toThrow(/unknown provider/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, defaults: { ...workflow.defaults, reasoning_effort: "extreme" } } }))).rejects.toThrow();
  });

  it("rejects step timeouts longer than the aggregate workflow timeout", async () => {
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ ...workflow.steps[0], timeout: "2h" }] } }))).rejects.toThrow(/timeout exceeds workflow.workflow_timeout/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ ...command, timeout: "2h" }, ...workflow.steps] } }))).rejects.toThrow(/timeout exceeds workflow.workflow_timeout/);
  });

  it("requires valid workflow-level timeout policy", async () => {
    const { workflow_timeout: _workflowTimeout, ...missingWorkflowTimeout } = workflow;
    const { step_timeout: _stepTimeout, ...missingStepTimeout } = workflow.defaults;
    await expect(loadWorkflowConfig(await project({ workflow: missingWorkflowTimeout }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, defaults: missingStepTimeout } }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, workflow_timeout: "0s" } }))).rejects.toThrow(/positive duration/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, workflow_timeout: "10m", defaults: { ...workflow.defaults, step_timeout: "15m" } } }))).rejects.toThrow(/must not exceed workflow_timeout/);
  });

  it("rejects legacy flat workflow defaults with migration guidance", async () => {
    const legacy = {
      version: 1, name: "default", workflow_timeout: "1h", default_step_timeout: "15m",
      harness: "codex", provider: "azure", model: "gpt-5.6-terra", steps: workflow.steps,
    };
    await expect(loadWorkflowConfig(await project({ workflow: legacy }))).rejects.toThrow();
  });

  it("accepts command steps anywhere and command-only workflows", async () => {
    const root = await project({ workflow: { ...workflow, steps: [command, ...workflow.steps, { ...command, id: "verify" }] } });
    const config = await loadWorkflowConfig(root);
    expect(config.workflow.steps).toEqual([command, { ...workflow.steps[0], allow_user_input: false, required_metrics: [] }, { ...command, id: "verify" }]);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [command] } }))).resolves.toBeDefined();
  });

  it("requires an interaction provider for user-input steps", async () => {
    const enabledWorkflow = { ...workflow, steps: [{ ...workflow.steps[0], allow_user_input: true }] };
    await expect(loadWorkflowConfig(await project({ workflow: enabledWorkflow }))).rejects.toThrow(/allow_user_input requires an interaction provider/);
    const slackRunner = {
      ...runner,
      deployment: { ...runner.deployment, global_fetch_strictly_public: true },
      interactions: {
        provider: "slack", live_wait: "30s", response_ttl: "24h", max_request_bytes: 65536, max_response_bytes: 65536,
        checkpoint: { max_files: 1000, max_file_bytes: 1048576, max_total_bytes: 8388608 },
        team_id: "T123", conversation_id: "C456", allowed_user_ids: ["U789"],
        bot_token_secret: "SLACK_BOT_TOKEN", signing_secret: "SLACK_SIGNING_SECRET",
      },
    };
    await expect(loadWorkflowConfig(await project({ runner: slackRunner, workflow: enabledWorkflow }))).resolves.toBeDefined();

    for (const type of ["pi", "opencode"] as const) {
      const unsupportedCatalog = { ...catalog, harnesses: { unsupported: { type } } };
      const unsupportedWorkflow = {
        ...enabledWorkflow,
        defaults: { ...enabledWorkflow.defaults, harness: "unsupported" },
      };
      await expect(loadWorkflowConfig(await project({ runner: slackRunner, catalog: unsupportedCatalog, workflow: unsupportedWorkflow })))
        .rejects.toThrow(new RegExp(`allow_user_input is unsupported for ${type}`));
    }
  });

  it("rejects malformed command steps", async () => {
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ id: "check", command: [" "] }, ...workflow.steps] } }))).rejects.toThrow(/executable/);
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ ...command, prompt: "run.md" }, ...workflow.steps] } }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ ...command, harness: "codex" }, ...workflow.steps] } }))).rejects.toThrow();
    await expect(loadWorkflowConfig(await project({ workflow: { ...workflow, steps: [{ ...command, reasoning_effort: "high" }, ...workflow.steps] } }))).rejects.toThrow();
  });


  it("rejects credential-bearing static headers", async () => {
    const invalid = structuredClone(catalog);
    invalid.providers.azure.static_headers = { Authorization: "secret" };
    await expect(loadRunnerConfig(await project({ catalog: invalid }))).rejects.toThrow(/credential injection/);
  });

  it("derives an R2-safe deterministic bucket name", () => {
    expect(storageBucketName("worker")).toBe("worker-storage");
    const long = storageBucketName("a".repeat(63));
    expect(long).toHaveLength(63);
    expect(long).toMatch(/^a{54}-[0-9a-f]{8}$/);
    expect(storageBucketName("a".repeat(63))).toBe(long);
  });

  it("isolates local Worker and R2 identities while preserving production names", async () => {
    const config = await loadRunnerConfig(await project());
    expect(resolveDeploymentTarget(config, "prod")).toEqual({
      profile: "prod", deploymentName: "runner", storageBucketName: "runner-storage",
    });
    expect(resolveDeploymentTarget(config, "local")).toEqual({
      profile: "local", deploymentName: "runner-local", storageBucketName: "runner-local-storage",
    });
    const long = environmentDeploymentName("a".repeat(63), "local");
    expect(long).toHaveLength(63);
    expect(long).toMatch(/^a{48}-[0-9a-f]{8}-local$/);
    expect(environmentDeploymentName("a".repeat(63), "local")).toBe(long);
  });

  it("generates profile-specific Worker and bucket bindings", async () => {
    const root = await project();
    for (const profile of ["local", "prod"] as const) {
      const directory = path.join(root, ".env.profiles", profile);
      await mkdir(directory, { recursive: true });
      await Promise.all([
        writeFile(path.join(directory, ".env.secrets"), ""),
        writeFile(path.join(directory, ".env.cloudflare"), ""),
      ]);
    }
    await generate(root, "local");
    const local = JSON.parse(await readFile(path.join(root, ".generated", "wrangler.jsonc"), "utf8"));
    expect(local.name).toBe("runner-local");
    expect(local.r2_buckets[0].bucket_name).toBe("runner-local-storage");
    await generate(root, "prod");
    const prod = JSON.parse(await readFile(path.join(root, ".generated", "wrangler.jsonc"), "utf8"));
    expect(prod.name).toBe("runner");
    expect(prod.r2_buckets[0].bucket_name).toBe("runner-storage");
  });


});
