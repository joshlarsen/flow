import { ContainerProxy } from "@cloudflare/containers";
import { AgentContainer } from "./container.ts";
import { JobCoordinator } from "./coordinator.ts";
import { OAuthCredentialBroker } from "./oauth-broker.ts";
import { ScheduleCoordinator } from "./scheduler.ts";
import { WorkflowBudgetCoordinator } from "./workflow-budget.ts";
import { handleScheduled } from "./scheduled.ts";
import { handleRequest } from "./api.ts";
import type { Env } from "./types.ts";

export { AgentContainer, ContainerProxy, handleRequest, JobCoordinator, OAuthCredentialBroker, ScheduleCoordinator, WorkflowBudgetCoordinator };

export default {
  fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    return handleRequest(request, env, ctx.tracing);
  },
  scheduled(controller: ScheduledController, env: Env): Promise<void> {
    return handleScheduled(controller, env);
  },
} satisfies ExportedHandler<Env>;
