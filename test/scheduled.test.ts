import { describe, expect, it, vi } from "vitest";
import { handleScheduled } from "../src/scheduled.ts";
import type { Env, WorkflowBundle } from "../src/types.ts";

const scheduledAt = Date.UTC(2026, 8, 14, 13);
const digest = "a".repeat(64);
const sortKey = "20260914T130000.000Z";
const manifestKey = `bundles/${sortKey}-${digest.slice(0, 12)}/manifest.json`;
const bundle = {
  manifest: {
    digest,
    sort_key: sortKey,
    workflow: { name: "default" },
    archive: { key: manifestKey.replace(/manifest\.json$/, "bundle.tgz") },
  },
} as WorkflowBundle;

function controller(cron: string, time = scheduledAt) {
  return { cron, scheduledTime: time, type: "scheduled", noRetry: vi.fn() } as unknown as ScheduledController;
}

function environment(enqueue = vi.fn(async () => undefined)) {
  const scheduler = { enqueue };
  return {
    env: {
      SCHEDULE_COORDINATOR: {
        idFromName: vi.fn((name: string) => name),
        get: vi.fn(() => scheduler),
      },
    } as unknown as Env,
    enqueue,
  };
}

describe("scheduled handler", () => {
  it("pins and durably enqueues a deployed schedule before suppressing retries", async () => {
    const { env, enqueue } = environment();
    const event = controller("0 13 * * MON-FRI");
    const load = vi.fn(async () => bundle);
    await handleScheduled(event, env, [{ id: "weekday", cron: event.cron }], load);
    expect(load).toHaveBeenCalledWith(env);
    expect(enqueue).toHaveBeenCalledWith({
      scheduleId: "weekday",
      cron: event.cron,
      scheduledAt,
      workflow: "default",
      workflowDigest: digest,
      manifestKey,
    });
    expect(event.noRetry).toHaveBeenCalledOnce();
  });

  it("ignores a stale propagated trigger without loading workflow state", async () => {
    const { env, enqueue } = environment();
    const event = controller("0 12 * * *");
    const load = vi.fn(async () => bundle);
    await handleScheduled(event, env, [{ id: "current", cron: "0 13 * * *" }], load);
    expect(load).not.toHaveBeenCalled();
    expect(enqueue).not.toHaveBeenCalled();
    expect(event.noRetry).toHaveBeenCalledOnce();
  });

  it("leaves the invocation failed when durable enqueueing fails", async () => {
    const enqueue = vi.fn(async () => { throw new Error("storage unavailable"); });
    const { env } = environment(enqueue);
    const event = controller("0 13 * * *");
    await expect(handleScheduled(event, env, [{ id: "daily", cron: event.cron }], async () => bundle)).rejects.toThrow("storage unavailable");
    expect(event.noRetry).not.toHaveBeenCalled();
  });

  it("rejects an invalid platform timestamp before reading the active bundle", async () => {
    const { env } = environment();
    const event = controller("0 13 * * *", Number.NaN);
    const load = vi.fn(async () => bundle);
    await expect(handleScheduled(event, env, [{ id: "daily", cron: event.cron }], load)).rejects.toThrow(/scheduled time/);
    expect(load).not.toHaveBeenCalled();
  });
});
