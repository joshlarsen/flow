import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import readline from "node:readline";

const models = [
  { id: "claude-opus-4-7", customModelOption: false },
  { id: "claude-opus-4-8", customModelOption: true },
  { id: "claude-sonnet-5", customModelOption: true },
  { id: "claude-opus-5", customModelOption: true },
  { id: "claude-haiku-5", customModelOption: true },
  { id: "claude-fable-5-1", customModelOption: true },
];

/** Builds the isolated environment shared by the ACP and request-shape probes. */
function probeEnvironment(model, configDir, baseURL, customModelOption) {
  const environment = {
    ...process.env,
    ANTHROPIC_AUTH_TOKEN: "probe-token",
    ANTHROPIC_BASE_URL: baseURL,
    ANTHROPIC_MODEL: model,
    CLAUDE_CODE_EFFORT_LEVEL: "xhigh",
    CLAUDE_CODE_EXECUTABLE: claudePath,
    CLAUDE_CODE_REMOTE: "1",
    CLAUDE_CONFIG_DIR: configDir,
    CLAUDE_MODEL_CONFIG: JSON.stringify({ availableModels: [model] }),
    DISABLE_TELEMETRY: "1",
    IS_SANDBOX: "1",
    NO_BROWSER: "1",
    TEMP: configDir,
    TMP: configDir,
    TMPDIR: configDir,
  };
  if (customModelOption) {
    environment.ANTHROPIC_CUSTOM_MODEL_OPTION = model;
  }
  return environment;
}

/** Waits for a child process with a hard timeout and guaranteed termination. */
async function waitForExit(child, timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`process ${child.pid ?? "unknown"} timed out`));
    }, timeoutMs);
    child.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once("close", (code, signal) => {
      clearTimeout(timer);
      resolve({ code, signal });
    });
  });
}

/** Confirms the pinned ACP adapter exposes every modern reasoning level. */
async function probeACP(model, configDir, customModelOption) {
  const child = spawn(acpPath, [], {
    env: probeEnvironment(model, configDir, "http://127.0.0.1:1", customModelOption),
    stdio: ["pipe", "pipe", "pipe"],
  });
  const exit = waitForExit(child, 10_000);
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
  const lines = readline.createInterface({ input: child.stdout });
  const pending = new Map();
  lines.on("line", (line) => {
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      return;
    }
    if (message.id !== undefined && pending.has(message.id)) {
      pending.get(message.id)(message);
      pending.delete(message.id);
    } else if (message.id !== undefined) {
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id: message.id, error: { code: -32601, message: "Not implemented" } })}\n`);
    }
  });
  let requestID = 0;
  const request = (method, params) => new Promise((resolve, reject) => {
    const id = ++requestID;
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, 15_000);
    pending.set(id, (message) => {
      clearTimeout(timer);
      if (message.error) reject(new Error(`${method}: ${JSON.stringify(message.error)}`));
      else resolve(message.result);
    });
    child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params })}\n`);
  });

  try {
    await request("initialize", {
      protocolVersion: 1,
      clientCapabilities: {},
      clientInfo: { name: "claude-capability-probe", version: "1" },
    });
    const session = await request("session/new", { cwd: process.cwd(), mcpServers: [] });
    const modelOption = session.configOptions.find((option) => option.id === "model");
    const effortOption = session.configOptions.find((option) => option.id === "effort");
    const effortValues = effortOption?.options?.map((option) => option.value) ?? [];
    if (modelOption?.currentValue !== model || !effortValues.includes("xhigh") || !effortValues.includes("max")) {
      throw new Error(`${model} ACP capabilities were incomplete: ${JSON.stringify(session.configOptions)}`);
    }
  } finally {
    child.stdin.end();
    const result = await exit.catch((error) => {
      throw new Error(`${error.message}; stderr: ${stderr.slice(-1000)}`);
    });
    if (result.code !== 0) {
      throw new Error(`${model} ACP probe exited unexpectedly: ${JSON.stringify(result)}; ${stderr.slice(-1000)}`);
    }
  }
}

/** Captures one request and verifies the pinned CLI emits the modern Anthropic shape. */
async function probeRequest(model, configDir, customModelOption) {
  let captured;
  const server = http.createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      if (request.method === "HEAD") {
        response.writeHead(200).end();
        return;
      }
      try {
        captured = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      } catch (error) {
        response.writeHead(400, { "content-type": "application/json" }).end();
        return;
      }
      response.writeHead(400, { "content-type": "application/json" });
      response.end(JSON.stringify({ type: "error", error: { type: "invalid_request_error", message: "probe complete" } }));
    });
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("probe server did not bind a TCP port");
  const child = spawn(claudePath, [
    "--print", "reply with ok",
    "--output-format", "json",
    "--model", model,
    "--effort", "xhigh",
    "--tools", "",
    "--settings", JSON.stringify({ availableModels: [model] }),
  ], {
    env: probeEnvironment(model, configDir, `http://127.0.0.1:${address.port}`, customModelOption),
    stdio: ["ignore", "ignore", "pipe"],
  });
  const exit = waitForExit(child, 15_000);
  let stderr = "";
  child.stderr.on("data", (chunk) => { stderr += chunk.toString(); });
  try {
    await exit;
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
  if (captured?.model !== model || captured?.thinking?.type !== "adaptive" || captured?.output_config?.effort !== "xhigh") {
    throw new Error(`${model} emitted an invalid request: ${JSON.stringify(captured)}; ${stderr.slice(-1000)}`);
  }
}

const [acpPath, claudePath] = process.argv.slice(2);
if (!acpPath || !claudePath) {
  console.error("Usage: node probe-claude-code-model-capabilities.mjs <claude-agent-acp> <claude-code-executable>");
  process.exit(2);
}

const temporaryRoot = await mkdtemp(path.join(os.tmpdir(), "claude-capability-probe-"));
try {
  for (const model of models) {
    const configDir = path.join(temporaryRoot, model.id);
    await mkdir(configDir, { recursive: true });
    await probeACP(model.id, configDir, model.customModelOption);
    await probeRequest(model.id, configDir, model.customModelOption);
  }
  console.log(`Verified modern Claude capabilities for ${models.map((model) => model.id).join(", ")}`);
} finally {
  await rm(temporaryRoot, { recursive: true, force: true });
}
