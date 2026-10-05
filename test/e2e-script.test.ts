import { describe, expect, it } from "vitest";
import { isLocalRunnerURL, requireEnvironment, requireEventDelivery, requireMemoryPersistence, requireTokenUsage, requireWorkflowVersion, resolveRunnerURL } from "../scripts/e2e.ts";

describe("E2E environment validation", () => {
  it("lists every missing configured secret", () => {
    expect(() => requireEnvironment(["RUNNER_API_TOKEN", "OPENAI_API_KEY"], {})).toThrow("Missing required environment variables: RUNNER_API_TOKEN, OPENAI_API_KEY");
  });

  it("deduplicates and trims configured secrets", () => {
    expect(requireEnvironment(["TOKEN", "TOKEN"], { TOKEN: " value " })).toEqual({ TOKEN: "value" });
  });
});

describe("E2E runner URL", () => {
  it("defaults to the local Worker and emits an informational message", () => {
    const messages: string[] = [];
    expect(resolveRunnerURL({}, (message) => messages.push(message)).href).toBe("http://127.0.0.1:8787/");
    expect(messages).toEqual(["INFO RUNNER_URL is not set; using http://127.0.0.1:8787."]);
  });

  it("allows loopback HTTP and requires HTTPS elsewhere", () => {
    const messages: string[] = [];
    expect(resolveRunnerURL({ RUNNER_URL: "http://localhost:8787" }, (message) => messages.push(message)).origin).toBe("http://localhost:8787");
    expect(messages).toEqual(["INFO Using RUNNER_URL http://localhost:8787."]);
    expect(resolveRunnerURL({ RUNNER_URL: "https://runner.example.com" }, () => {}).origin).toBe("https://runner.example.com");
    expect(() => resolveRunnerURL({ RUNNER_URL: "http://runner.example.com" }, () => {})).toThrow(/must use HTTPS/);
    expect(isLocalRunnerURL(new URL("http://localhost:8787"))).toBe(true);
    expect(isLocalRunnerURL(new URL("https://runner.example.com"))).toBe(false);
  });
});

describe("E2E result validation", () => {
  it("rejects an active workflow that differs from local configuration", () => {
    expect(() => requireWorkflowVersion({ workflow_version: "old" }, "new")).toThrow("run pnpm bundle:sync");
    expect(() => requireWorkflowVersion({ workflow_version: "same" }, "same")).not.toThrow();
  });

  it("accepts normalized token counters", () => {
    expect(() => requireTokenUsage({ result: { usage: { total_tokens: 100, input_tokens: 90, output_tokens: 10 } } })).not.toThrow();
  });

  it("rejects a successful result without token counters", () => {
    expect(() => requireTokenUsage({ result: { usage: null } })).toThrow("Successful job did not return token usage");
  });

  it("validates persistent memory metadata", () => {
    const memory = { key: "memory/default.sqlite3", size: 4096, sha256: "a".repeat(64), etag: "etag", persisted_at: "2026-09-13T23:53:32.042Z" };
    expect(() => requireMemoryPersistence({ result: { memory } })).not.toThrow();
    expect(() => requireMemoryPersistence({ result: { memory_error: { code: "memory_persist_failed" } } })).toThrow(/memory error/);
    expect(() => requireMemoryPersistence({ result: { memory: { ...memory, size: 25 * 1024 * 1024 + 1 } } })).toThrow(/invalid persistence metadata/);
    expect(() => requireMemoryPersistence({ result: { memory: { ...memory, persisted_at: "not-a-date" } } })).toThrow(/invalid persistence metadata/);
  });

  it("waits for pending history archival and rejects failed or disabled delivery", () => {
    expect(requireEventDelivery({ history: { state: "pending" } })).toBe(false);
    expect(requireEventDelivery({ history: { state: "retrying" } })).toBe(false);
    expect(requireEventDelivery({ history: { state: "archived" } })).toBe(true);
    expect(() => requireEventDelivery({ history: { state: "failed" } })).toThrow("history archival failed");
    expect(() => requireEventDelivery({ history: { state: "disabled" } })).toThrow("history archival is disabled");
    expect(() => requireEventDelivery({})).toThrow("did not return event delivery state");
  });
});
