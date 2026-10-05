import { describe, expect, it, vi } from "vitest";
import { FlowClient } from "../tools/flow-client.ts";
const token = "a".repeat(32);
describe("operator client", () => {
  it("uses bearer auth and preserves idempotency and pagination", async () => {
    const request = vi.fn<typeof fetch>(async () => new Response("{}"));
    const client = new FlowClient("https://runner.test", token, request);
    await client.run("repeat");
    expect(request.mock.calls[0]![1]).toMatchObject({
      method: "POST",
      body: "{}",
      headers: {
        authorization: `Bearer ${token}`,
        "idempotency-key": "repeat",
      },
    });
    await client.page("events", "job", "abc", 200);
    expect(String(request.mock.calls[1]![0])).toContain(
      "/v1/jobs/job/events?limit=200&cursor=abc",
    );
  });
  it("does not cancel a sleeping job when an operator wait expires", async () => {
    const request = vi.fn<typeof fetch>(
      async () => new Response(JSON.stringify({ status: "sleeping" })),
    );
    const client = new FlowClient("https://runner.test", token, request);
    await expect(client.wait("job", 1, 2)).rejects.toThrow(/remains sleeping/);
    expect(request.mock.calls.every((call) => !call[1]?.method)).toBe(true);
  });
  it("rejects unsafe origins and traversal before requesting an artifact", async () => {
    expect(() => new FlowClient("http://example.com", token)).toThrow(/HTTPS/);
    const request = vi.fn<typeof fetch>();
    await expect(
      new FlowClient("https://runner.test", token, request).artifact(
        "job",
        "../secret",
      ),
    ).rejects.toThrow(/path/);
    expect(request).not.toHaveBeenCalled();
  });
});
