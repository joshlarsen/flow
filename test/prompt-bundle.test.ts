import { chmod, mkdtemp, mkdir, readFile, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { describe, expect, it } from "vitest";
import { loadRunnerConfig, type WorkflowRunnerConfig } from "../tools/config.ts";
import { buildWorkflowBundle, reconcileWorkflowBudget, requiredMetricInstructions, syncWorkflowBundle } from "../tools/workflow-bundle.ts";

async function testConfig(): Promise<WorkflowRunnerConfig> {
  return {
    ...await loadRunnerConfig(),
    workflow: {
      version: 1,
      name: "default",
      memory: false,
      workflow_timeout: "1h",
      schedules: [],
      defaults: {
        step_timeout: "15m",
        harness: "pi",
        provider: "openai",
        model: "gpt-5.6-luna",
        reasoning_effort: "medium",
      },
      steps: [
        { id: "check", command: ["git", "--version"], timeout: "30s" },
        { id: "run", prompt: "run.md", allow_user_input: false, required_metrics: [] },
        { id: "review", prompt: "review.md", allow_user_input: false, required_metrics: [], model: "gpt-5.6-sol", reasoning_effort: null, timeout: "5m" },
      ],
    },
  };
}

describe("workflow bundles", () => {
  it("renders deterministic required-metric prompt instructions", () => {
    const instructions = requiredMetricInstructions([
      { namespace: "haiku", key: "num_lines", description: "number of lines in the written haiku" },
      { namespace: "haiku", key: "score", description: "quality score" },
    ]);
    expect(instructions).toContain("## Required metrics");
    expect(instructions).toContain("- `haiku.num_lines`: number of lines in the written haiku");
    expect(instructions).toContain("- `haiku.score`: quality score");
    expect(instructions).toContain('{"metrics":["haiku.num_lines=<number>","haiku.score=<number>"]}');
    expect(requiredMetricInstructions([])).toBe("");
  });

  it("authenticates budget reconciliation without putting the token in the URL", async () => {
    const request = async (input: string | URL | globalThis.Request, init?: RequestInit) => {
      expect(String(input)).toBe("https://runner.example.com/v1/workflow-budget/reconcile");
      expect(init?.method).toBe("POST");
      expect(new Headers(init?.headers).get("authorization")).toBe(`Bearer ${"t".repeat(32)}`);
      return new Response(JSON.stringify({ allowed: true }), { status: 200 });
    };
    await expect(reconcileWorkflowBudget("https://runner.example.com/", "t".repeat(32), request as typeof fetch)).resolves.toBeUndefined();
  });

  it("builds an archive and deterministic semantic manifest", async () => {
    const config = await testConfig();
    const root = await mkdtemp(path.join(tmpdir(), "prompt-bundle-"));
    await mkdir(path.join(root, "config", "prompts"), { recursive: true });
    for (const step of config.workflow.steps) {
      if (!("prompt" in step)) continue;
      const promptPath = path.join(root, "config", "prompts", ...step.prompt.split("/"));
      await mkdir(path.dirname(promptPath), { recursive: true });
      await writeFile(promptPath, `prompt for ${step.id}\n`);
    }
    await mkdir(path.join(root, "config", "scripts"), { recursive: true });
    await writeFile(path.join(root, "config", "scripts", "check.sh"), "#!/bin/sh\ntrue\n");
    await chmod(path.join(root, "config", "scripts", "check.sh"), 0o755);
    await writeFile(path.join(root, "config", "scripts", "empty.txt"), "");
    await writeFile(path.join(root, "config", "scripts", ".settings"), "enabled\n");
    await mkdir(path.join(root, "config", "skills", "example"), { recursive: true });
    await writeFile(path.join(root, "config", "skills", ".keep"), "");
    await writeFile(path.join(root, "config", "skills", "example", "SKILL.md"), "# Example\n");
    const first = await buildWorkflowBundle(config, root);
    const second = await buildWorkflowBundle(config, root);
    expect(first.manifest.digest).toBe(second.manifest.digest);
    expect(first.manifest.workflow.workflow_timeout_ms).toBe(3600000);
    expect(first.manifest.workflow.memory_enabled).toBe(false);
    expect(first.manifest.workflow.default_step_timeout_ms).toBe(900000);
    expect(first.manifest.workflow.default_harness).toBe("pi");
    expect(first.manifest.workflow.default_model).toBe("gpt-5.6-luna");
    expect(first.manifest.workflow.default_reasoning_effort).toBe("medium");
    expect(first.manifest.workflow.steps[0]).toEqual({ id: "check", command: ["git", "--version"], timeout_ms: 30000 });
    expect(first.manifest.workflow.steps[1]).toEqual({ id: "run", prompt: "run.md", allow_user_input: false, harness: "pi", provider: "openai", model: "gpt-5.6-luna", reasoning_effort: "medium", required_metrics: [], timeout_ms: 900000 });
    expect(first.manifest.workflow.steps[2]).toEqual({ id: "review", prompt: "review.md", allow_user_input: false, harness: "pi", provider: "openai", model: "gpt-5.6-sol", reasoning_effort: null, required_metrics: [], timeout_ms: 300000 });
    expect(first.manifest.archive.key).toContain(first.manifest.digest.slice(0, 12));
    expect(first.manifest.archive.key).toBe(`bundles/${first.manifest.sort_key}-${first.manifest.digest.slice(0, 12)}/bundle.tgz`);
    expect(first.manifest.sort_key).toMatch(/^\d{8}T\d{6}\.\d{3}Z$/);
    expect(first.manifest.files).toEqual(expect.arrayContaining([
      expect.objectContaining({ kind: "script", path: "check.sh", executable: true }),
      expect.objectContaining({ kind: "script", path: "empty.txt", size: 0 }),
      expect.objectContaining({ kind: "script", path: ".settings" }),
      expect.objectContaining({ kind: "skill", path: "example/SKILL.md" }),
    ]));
    expect(first.manifest.files.some((file) => file.path === ".keep")).toBe(false);
    expect((await readFile(first.archivePath)).byteLength).toBeGreaterThan(0);

    const target = { profile: "local" as const, deploymentName: "runner-local", storageBucketName: "runner-local-storage" };
    const commands: string[][] = [];
    const command = async (args: string[], accountId: string) => {
      expect(accountId).toBe("a".repeat(32));
      commands.push(args);
      return 0;
    };
    await syncWorkflowBundle(config, target, "a".repeat(32), root, true, command);
    expect(commands[0]).toEqual(["r2", "bucket", "info", "runner-local-storage"]);
    expect(commands.slice(1).map((args) => args[3])).toEqual(expect.arrayContaining([
      expect.stringMatching(/^runner-local-storage\/bundles\/.*\/bundle\.tgz$/),
      expect.stringMatching(/^runner-local-storage\/bundles\/.*\/manifest\.json$/),
      "runner-local-storage/active.json",
    ]));
    expect(commands.flat().join(" ")).not.toContain("runner-storage/");


    config.workflow.steps[0] = { id: "check", command: ["git", "status"], timeout: "30s" };
    expect((await buildWorkflowBundle(config, root)).manifest.digest).not.toBe(first.manifest.digest);
    config.workflow.steps[0] = { id: "check", command: ["git", "--version"], timeout: "30s" };
    config.workflow.workflow_timeout = "2h";
    expect((await buildWorkflowBundle(config, root)).manifest.digest).not.toBe(first.manifest.digest);
    config.workflow.workflow_timeout = "1h";
    config.workflow.defaults.model = "gpt-5.6-sol";
    expect((await buildWorkflowBundle(config, root)).manifest.digest).not.toBe(first.manifest.digest);
    config.workflow.defaults.model = "gpt-5.6-luna";
    config.workflow.memory = true;
    expect((await buildWorkflowBundle(config, root)).manifest.digest).not.toBe(first.manifest.digest);
    config.workflow.memory = false;
    delete config.workflow.defaults.reasoning_effort;
    const withoutReasoningDefault = await buildWorkflowBundle(config, root);
    expect(withoutReasoningDefault.manifest.workflow.default_reasoning_effort).toBeNull();
    expect(withoutReasoningDefault.manifest.workflow.steps[1]).toEqual(expect.objectContaining({ reasoning_effort: null }));
  });

  it("rejects prompts whose generated metric instructions exceed max_prompt_bytes", async () => {
    const config = await testConfig();
    const step = config.workflow.steps[1];
    if (!(step && "prompt" in step)) throw new Error("missing test agent step");
    step.required_metrics = [{ namespace: "haiku", key: "num_lines", description: "number of lines" }];
    config.runner.max_prompt_bytes = 100;
    const root = await mkdtemp(path.join(tmpdir(), "prompt-bundle-metrics-"));
    await mkdir(path.join(root, "config", "prompts"), { recursive: true });
    await writeFile(path.join(root, "config", "prompts", "run.md"), "read the haiku\n");
    await writeFile(path.join(root, "config", "prompts", "review.md"), "review it\n");
    await expect(buildWorkflowBundle(config, root)).rejects.toThrow(/required metric instructions.*max_prompt_bytes/);
  });

  it("rejects missing workflow prompts while allowing absent asset directories", async () => {
    const config = await testConfig();
    const missingRoot = await mkdtemp(path.join(tmpdir(), "prompt-bundle-"));
    await mkdir(path.join(missingRoot, "config", "prompts"), { recursive: true });
    await expect(buildWorkflowBundle(config, missingRoot)).rejects.toThrow(/missing prompt/);
  });

  it("follows repository-confined symlinks but rejects untracked external targets", async () => {
    const config = await testConfig();
    const root = await mkdtemp(path.join(tmpdir(), "workflow-bundle-git-"));
    await mkdir(path.join(root, "config", "prompts"), { recursive: true });
    await mkdir(path.join(root, "config", "scripts"), { recursive: true });
    for (const step of config.workflow.steps) {
      if ("prompt" in step) await writeFile(path.join(root, "config", "prompts", step.prompt), "prompt\n");
    }
    await mkdir(path.join(root, "shared"));
    await writeFile(path.join(root, "shared", "tool.sh"), "#!/bin/sh\ntrue\n");
    spawnSync("git", ["init", "-q"], { cwd: root });
    spawnSync("git", ["add", "shared/tool.sh"], { cwd: root });
    await symlink(path.join(root, "shared"), path.join(root, "config", "scripts", "shared"));
    expect((await buildWorkflowBundle(config, root)).manifest.files).toEqual(expect.arrayContaining([
      expect.objectContaining({ kind: "script", path: "shared/tool.sh" }),
    ]));

    await writeFile(path.join(root, "private.txt"), "not tracked");
    await symlink(path.join(root, "private.txt"), path.join(root, "config", "scripts", "private.txt"));
    await expect(buildWorkflowBundle(config, root)).rejects.toThrow(/untracked file private\.txt/);
  });
});
