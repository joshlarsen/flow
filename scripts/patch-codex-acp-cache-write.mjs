import { chmod, readFile, rename, stat, writeFile } from "node:fs/promises";
import path from "node:path";

const supportedVersion = "1.11.0";

const tokenCountBefore = `function toTokenCount(usage) {
  return {
    totalTokens: usage.totalTokens,
    inputTokens: usage.inputTokens - usage.cachedInputTokens,
    cachedInputTokens: usage.cachedInputTokens,
    outputTokens: usage.outputTokens,
    reasoningOutputTokens: usage.reasoningOutputTokens
  };
}`;

const tokenCountAfter = `function toTokenCount(usage) {
  const cacheWriteInputTokens = usage.cacheWriteInputTokens ?? 0;
  return {
    totalTokens: usage.totalTokens,
    inputTokens: usage.inputTokens - usage.cachedInputTokens - cacheWriteInputTokens,
    cachedInputTokens: usage.cachedInputTokens,
    cacheWriteInputTokens,
    outputTokens: usage.outputTokens,
    reasoningOutputTokens: usage.reasoningOutputTokens
  };
}`;

const promptUsageBefore = `    cachedReadTokens: tokenCount.cachedInputTokens,
    outputTokens: tokenCount.outputTokens,`;

const promptUsageAfter = `    cachedReadTokens: tokenCount.cachedInputTokens,
    cachedWriteTokens: tokenCount.cacheWriteInputTokens,
    outputTokens: tokenCount.outputTokens,`;

const promptUsageCallBefore = "this.buildPromptUsage(sessionState.lastTokenUsage)";
const promptUsageCallAfter =
  "this.buildPromptUsage(sessionState.totalTokenUsage ?? sessionState.lastTokenUsage)";

const quotaBefore = `  buildQuotaMeta(sessionState) {
    const lastTokenUsage = sessionState.lastTokenUsage;
    const modelName = sessionState.currentModelId.replace(/\\[.*?]$/, "");
    const modelUsage = lastTokenUsage != null ? [{ model: modelName, token_count: lastTokenUsage }] : [];
    return {
      quota: {
        token_count: sessionState.lastTokenUsage,
        model_usage: modelUsage
      }
    };
  }`;

const quotaAfter = `  buildQuotaMeta(sessionState) {
    const promptTokenUsage = sessionState.totalTokenUsage ?? sessionState.lastTokenUsage;
    const modelName = sessionState.currentModelId.replace(/\\[.*?]$/, "");
    const modelUsage = promptTokenUsage != null ? [{ model: modelName, token_count: promptTokenUsage }] : [];
    return {
      quota: {
        token_count: promptTokenUsage,
        model_usage: modelUsage
      }
    };
  }`;

/** Replaces one known bundle fragment and rejects source drift or double application. */
function replaceExactlyOnce(source, before, after, label) {
  const matches = source.split(before).length - 1;
  if (matches !== 1) {
    const reason = source.includes(after) ? "patch is already applied" : `found ${matches} matching fragments`;
    throw new Error(`Cannot patch ${label}: ${reason}`);
  }
  return source.replace(before, after);
}

/** Replaces an expected number of identical bundle fragments. */
function replaceExactly(source, before, after, expected, label) {
  const matches = source.split(before).length - 1;
  if (matches !== expected) {
    const reason = source.includes(after) ? "patch is already applied" : `found ${matches} matching fragments`;
    throw new Error(`Cannot patch ${label}: ${reason}`);
  }
  return source.split(before).join(after);
}

/** Applies version-pinned token-accounting fixes to an installed codex-acp package. */
async function patchCodexAcp(packageRoot) {
  const packagePath = path.join(packageRoot, "package.json");
  const manifest = JSON.parse(await readFile(packagePath, "utf8"));
  if (manifest.name !== "@agentclientprotocol/codex-acp") {
    throw new Error(`Unexpected package name ${JSON.stringify(manifest.name)}`);
  }
  if (manifest.version !== supportedVersion) {
    throw new Error(
      `Token-accounting patch supports codex-acp ${supportedVersion}, found ${JSON.stringify(manifest.version)}`,
    );
  }

  const bundlePath = path.join(packageRoot, "dist", "index.js");
  const bundleMode = (await stat(bundlePath)).mode;
  const source = await readFile(bundlePath, "utf8");
  const withTokenCount = replaceExactlyOnce(
    source,
    tokenCountBefore,
    tokenCountAfter,
    "toTokenCount",
  );
  const patched = replaceExactlyOnce(
    withTokenCount,
    promptUsageBefore,
    promptUsageAfter,
    "toPromptUsage",
  );
  const withPromptTotals = replaceExactly(
    patched,
    promptUsageCallBefore,
    promptUsageCallAfter,
    4,
    "prompt usage call sites",
  );
  const withQuotaTotals = replaceExactlyOnce(
    withPromptTotals,
    quotaBefore,
    quotaAfter,
    "quota usage",
  );
  const temporaryPath = `${bundlePath}.cache-write-patch`;
  await writeFile(temporaryPath, withQuotaTotals, "utf8");
  await chmod(temporaryPath, bundleMode);
  await rename(temporaryPath, bundlePath);
  console.log(`Patched token accounting in codex-acp ${supportedVersion}`);
}

const packageRoot = process.argv[2];
if (!packageRoot) {
  console.error("Usage: node patch-codex-acp-cache-write.mjs <codex-acp-package-root>");
  process.exit(2);
}

patchCodexAcp(packageRoot).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
