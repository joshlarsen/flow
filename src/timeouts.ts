
/** Derives the absolute aggregate deadline shared by the Worker and supervisor. */
export function workflowDeadlineAt(startedAt: number, workflowTimeoutMs: number): number {
  return startedAt + workflowTimeoutMs;
}

/** Checks the Worker-side aggregate deadline without relying on container idleness. */
export function workflowDeadlineElapsed(deadlineAt: number | null, now = Date.now()): boolean {
  return deadlineAt !== null && now >= deadlineAt;
}

/** Gives result collection two minutes beyond the workflow execution deadline. */
export function containerSleepAfter(workflowTimeoutMs: number): string {
  return `${Math.ceil((workflowTimeoutMs + 120_000) / 60_000)}m`;
}
