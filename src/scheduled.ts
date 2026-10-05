import { appConfig } from "./generated-config.ts";
import type { Env } from "./types.ts";
import { loadActiveWorkflowBundle, workflowManifestKey } from "./workflow.ts";

interface DeployedSchedule {
  readonly id: string;
  readonly cron: string;
}

/** Persists a known Cloudflare Cron occurrence before reporting trigger success. */
export async function handleScheduled(
  controller: ScheduledController,
  env: Env,
  schedules: ReadonlyArray<DeployedSchedule> = appConfig.workflowSchedules as ReadonlyArray<DeployedSchedule>,
  loadBundle: typeof loadActiveWorkflowBundle = loadActiveWorkflowBundle,
): Promise<void> {
  const schedule = schedules.find((item) => item.cron === controller.cron);
  if (!schedule) {
    console.warn(JSON.stringify({
      level: "warn",
      source: "worker",
      event: "scheduled_trigger_ignored",
      cron: controller.cron.slice(0, 256),
    }));
    controller.noRetry();
    return;
  }
  if (!Number.isSafeInteger(controller.scheduledTime) || controller.scheduledTime <= 0) {
    throw new Error("Cloudflare scheduled time is invalid");
  }
  const bundle = await loadBundle(env);
  const scheduler = env.SCHEDULE_COORDINATOR.get(env.SCHEDULE_COORDINATOR.idFromName("global"));
  await scheduler.enqueue({
    scheduleId: schedule.id,
    cron: schedule.cron,
    scheduledAt: controller.scheduledTime,
    workflow: bundle.manifest.workflow.name,
    workflowDigest: bundle.manifest.digest,
    manifestKey: workflowManifestKey(bundle.manifest),
  });
  controller.noRetry();
}
