import { execFile } from "node:child_process";
import { chmod, mkdtemp, mkdir, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { promisify } from "node:util";
import { describe, expect, it } from "vitest";

const execFileAsync = promisify(execFile);
const patchScript = path.resolve("scripts/patch-codex-acp-cache-write.mjs");
const originalBundle = `function toTokenCount(usage) {
  return {
    totalTokens: usage.totalTokens,
    inputTokens: usage.inputTokens - usage.cachedInputTokens,
    cachedInputTokens: usage.cachedInputTokens,
    outputTokens: usage.outputTokens,
    reasoningOutputTokens: usage.reasoningOutputTokens
  };
}
function toPromptUsage(tokenCount) {
  return {
    totalTokens: tokenCount.totalTokens,
    inputTokens: tokenCount.inputTokens,
    cachedReadTokens: tokenCount.cachedInputTokens,
    outputTokens: tokenCount.outputTokens,
    thoughtTokens: tokenCount.reasoningOutputTokens
  };
}
const promptResults = [
  this.buildPromptUsage(sessionState.lastTokenUsage),
  this.buildPromptUsage(sessionState.lastTokenUsage),
  this.buildPromptUsage(sessionState.lastTokenUsage),
  this.buildPromptUsage(sessionState.lastTokenUsage)
];
class Agent {
  buildQuotaMeta(sessionState) {
    const lastTokenUsage = sessionState.lastTokenUsage;
    const modelName = sessionState.currentModelId.replace(/\\[.*?]$/, "");
    const modelUsage = lastTokenUsage != null ? [{ model: modelName, token_count: lastTokenUsage }] : [];
    return {
      quota: {
        token_count: sessionState.lastTokenUsage,
        model_usage: modelUsage
      }
    };
  }
}`;

async function packageFixture(version = "1.11.0", bundle = originalBundle): Promise<string> {
  const packageRoot = await mkdtemp(path.join(tmpdir(), "codex-acp-patch-test-"));
  await mkdir(path.join(packageRoot, "dist"));
  await writeFile(
    path.join(packageRoot, "package.json"),
    JSON.stringify({ name: "@agentclientprotocol/codex-acp", version }),
  );
  const bundlePath = path.join(packageRoot, "dist", "index.js");
  await writeFile(bundlePath, bundle);
  await chmod(bundlePath, 0o755);
  return packageRoot;
}

describe("codex-acp cache-write patch", () => {
  it("reports all prompt calls and separates cache writes from ordinary input", async () => {
    const packageRoot = await packageFixture();

    const result = await execFileAsync(process.execPath, [patchScript, packageRoot]);
    const patched = await readFile(path.join(packageRoot, "dist", "index.js"), "utf8");

    expect(result.stdout).toContain("Patched token accounting in codex-acp 1.11.0");
    expect(patched).toContain("const cacheWriteInputTokens = usage.cacheWriteInputTokens ?? 0;");
    expect(patched).toContain(
      "inputTokens: usage.inputTokens - usage.cachedInputTokens - cacheWriteInputTokens,",
    );
    expect(patched).toContain("cacheWriteInputTokens,");
    expect(patched).toContain("cachedWriteTokens: tokenCount.cacheWriteInputTokens,");
    expect(patched.match(/sessionState\.totalTokenUsage \?\? sessionState\.lastTokenUsage/g)).toHaveLength(5);
    expect(patched).toContain("token_count: promptTokenUsage,");
    expect((await stat(path.join(packageRoot, "dist", "index.js"))).mode & 0o777).toBe(0o755);
  });

  it("rejects an unsupported codex-acp version", async () => {
    const packageRoot = await packageFixture("1.11.1");

    await expect(execFileAsync(process.execPath, [patchScript, packageRoot])).rejects.toMatchObject({
      stderr: expect.stringContaining("supports codex-acp 1.11.0"),
    });
  });

  it("rejects source drift instead of applying a partial patch", async () => {
    const packageRoot = await packageFixture("1.11.0", originalBundle.replace(
      "inputTokens: usage.inputTokens - usage.cachedInputTokens,",
      "inputTokens: usage.inputTokens,",
    ));

    await expect(execFileAsync(process.execPath, [patchScript, packageRoot])).rejects.toMatchObject({
      stderr: expect.stringContaining("Cannot patch toTokenCount"),
    });
    expect(await readFile(path.join(packageRoot, "dist", "index.js"), "utf8")).not.toContain(
      "cachedWriteTokens: tokenCount.cacheWriteInputTokens,",
    );
  });
});
