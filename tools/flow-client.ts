import type { JobView } from "../src/types.ts";
export const terminalStatuses = new Set([
  "succeeded",
  "partial",
  "failed",
  "timed_out",
  "cancelled",
  "interrupted",
]);
/** Operator client shared by CLI and end-to-end verification. */
export class FlowClient {
  readonly origin: URL;
  constructor(
    origin: string,
    private token: string,
    private request: typeof fetch = fetch,
  ) {
    this.origin = new URL(origin);
    const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(
      this.origin.hostname,
    );
    if (
      (this.origin.protocol !== "https:" &&
        !(this.origin.protocol === "http:" && loopback)) ||
      this.origin.username ||
      this.origin.password ||
      this.origin.pathname !== "/" ||
      this.origin.search ||
      this.origin.hash
    )
      throw new Error(
        "Runner URL must be an HTTPS origin (HTTP loopback is allowed)",
      );
    if (token.length < 32)
      throw new Error("API token must contain at least 32 characters");
  }
  async call(path: string, options: RequestInit = {}): Promise<Response> {
    const response = await this.request(new URL(path, this.origin), {
      ...options,
      headers: { authorization: `Bearer ${this.token}`, ...options.headers },
      signal: options.signal ?? AbortSignal.timeout(30_000),
    });
    if (!response.ok) {
      const detail = (await response.text()).slice(0, 4096);
      throw new Error(`HTTP ${response.status}: ${detail}`);
    }
    return response;
  }
  async preflight() {
    const result = (await (await this.call("/v1/preflight")).json()) as {
      status?: string;
    };
    if (result.status !== "ready")
      throw new Error("Invalid readiness response");
    return result;
  }
  async run(
    idempotencyKey?: string,
  ): Promise<{ job_id: string; workflow_version: string }> {
    return (
      await this.call("/v1/jobs", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          ...(idempotencyKey ? { "idempotency-key": idempotencyKey } : {}),
        },
        body: "{}",
      })
    ).json();
  }
  async get(id: string): Promise<JobView> {
    return (await this.call(`/v1/jobs/${encodeURIComponent(id)}`)).json();
  }
  async cancel(id: string) {
    return (
      await this.call(`/v1/jobs/${encodeURIComponent(id)}`, {
        method: "DELETE",
      })
    ).json();
  }
  async page(
    resource: "jobs" | "events" | "traces" | "metrics",
    id?: string,
    cursor?: string,
    limit = 100,
  ) {
    const params = new URLSearchParams({ limit: String(limit) });
    if (cursor) params.set("cursor", cursor);
    const route =
      resource === "jobs"
        ? "/v1/jobs"
        : `/v1/jobs/${encodeURIComponent(id!)}/${resource}`;
    return (await this.call(`${route}?${params}`)).json();
  }
  async artifact(id: string, path: string): Promise<Response> {
    if (
      !path ||
      path.split("/").some((part) => !part || part === "." || part === "..") ||
      path.includes("\\")
    )
      throw new Error("Invalid artifact path");
    return this.call(
      `/v1/jobs/${encodeURIComponent(id)}/artifacts/${path.split("/").map(encodeURIComponent).join("/")}`,
    );
  }
  async wait(
    id: string,
    timeoutMs = 3_600_000,
    intervalMs = 2000,
  ): Promise<JobView> {
    const deadline = Date.now() + timeoutMs;
    while (true) {
      const job = await this.get(id);
      if (terminalStatuses.has(job.status)) return job;
      if (Date.now() >= deadline)
        throw new Error(`Wait timed out; job ${id} remains ${job.status}`);
      await new Promise((resolve) =>
        setTimeout(
          resolve,
          Math.min(intervalMs, Math.max(1, deadline - Date.now())),
        ),
      );
    }
  }
}
