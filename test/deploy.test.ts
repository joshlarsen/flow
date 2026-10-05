import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { assertProductionDeploymentProfile, prebuiltImageConfig, uploadBlobInChunks } from "../tools/deploy.ts";

describe("deploy configuration", () => {
  it("allows only the explicitly active production profile", () => {
    expect(() => assertProductionDeploymentProfile("prod")).not.toThrow();
    expect(() => assertProductionDeploymentProfile("local")).toThrow(/requires the active prod environment profile/);
    expect(() => assertProductionDeploymentProfile(null)).toThrow(/requires the active prod environment profile/);
  });

  it("replaces Dockerfile container images while preserving sibling-relative paths", async () => {
    const generated = await mkdtemp(path.join(tmpdir(), "deploy-config-test-"));
    const configPath = path.join(generated, "wrangler.jsonc");
    await writeFile(configPath, JSON.stringify({
      main: "../src/index.ts",
      containers: [{ class_name: "AgentContainer", image: "../Dockerfile" }],
    }));

    const image = "registry.cloudflare.com/account/agent-runner-agentcontainer:crane-upload";
    const deployConfigPath = await prebuiltImageConfig(configPath, image);
    const deployConfig = JSON.parse(await readFile(deployConfigPath, "utf8")) as {
      main: string;
      containers: Array<{ class_name: string; image: string }>;
    };

    expect(path.dirname(deployConfigPath)).toBe(path.dirname(configPath));
    expect(deployConfig.main).toBe("../src/index.ts");
    expect(deployConfig.containers).toEqual([{ class_name: "AgentContainer", image }]);
  });

  it.each([
    { reportedRange: "0-3", expectedRanges: ["0-3", "4-7", "8-9"] },
    { reportedRange: "bytes=0-3", expectedRanges: ["0-3", "4-7", "8-9"] },
    { reportedRange: "0--1", expectedRanges: ["0-3", "0-3", "4-7", "8-9"] },
    { reportedRange: "bytes=0--1", expectedRanges: ["0-3", "0-3", "4-7", "8-9"] },
  ])("uploads large blobs and resumes registry range $reportedRange", async ({ reportedRange, expectedRanges }) => {
    const directory = await mkdtemp(path.join(tmpdir(), "deploy-blob-test-"));
    const blobPath = path.join(directory, "blob");
    await writeFile(blobPath, "abcdefghij");
    const ranges: string[] = [];
    let firstPatch = true;
    const fetcher = async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
      const method = init?.method;
      if (method === "HEAD") return new Response(null, { status: 404 });
      if (method === "POST") {
        return new Response(null, { status: 202, headers: { location: "/v2/account/image/blobs/uploads/id?state=0" } });
      }
      if (method === "GET") {
        return new Response(null, {
          status: 204,
          headers: { location: "/v2/account/image/blobs/uploads/id?state=4", range: reportedRange },
        });
      }
      if (method === "PATCH") {
        const range = new Headers(init?.headers).get("content-range");
        if (!range) throw new Error("missing content range");
        ranges.push(range);
        if (firstPatch) {
          firstPatch = false;
          throw new Error("simulated dropped connection after acceptance");
        }
        return new Response(null, {
          status: 202,
          headers: { location: `/v2/account/image/blobs/uploads/id?state=${ranges.length}` },
        });
      }
      if (method === "PUT") {
        expect(String(input)).toContain("digest=sha256%3A");
        return new Response(null, { status: 201 });
      }
      throw new Error(`unexpected request method ${method}`);
    };

    await uploadBlobInChunks(
      "account/image",
      { digest: `sha256:${"a".repeat(64)}`, size: 10 },
      blobPath,
      { username: "user", password: "secret" },
      fetcher as typeof fetch,
      4,
    );

    expect(ranges).toEqual(expectedRanges);
  });

  it("retries transient blob checks and upload-status requests", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "deploy-blob-retry-test-"));
    const blobPath = path.join(directory, "blob");
    await writeFile(blobPath, "abcd");
    let heads = 0;
    let patches = 0;
    let statusChecks = 0;
    const waits: number[] = [];
    const fetcher = async (_input: string | URL | Request, init?: RequestInit): Promise<Response> => {
      if (init?.method === "HEAD") {
        heads += 1;
        if (heads === 1) throw new TypeError("fetch failed");
        return new Response(null, { status: 404 });
      }
      if (init?.method === "POST") {
        return new Response(null, { status: 202, headers: { location: "/v2/account/image/blobs/uploads/id" } });
      }
      if (init?.method === "PATCH") {
        patches += 1;
        if (patches === 1) throw new TypeError("fetch failed");
        return new Response(null, { status: 202, headers: { location: "/v2/account/image/blobs/uploads/id" } });
      }
      if (init?.method === "GET") {
        statusChecks += 1;
        if (statusChecks === 1) throw new TypeError("fetch failed");
        return new Response(null, { status: 204, headers: { location: "/v2/account/image/blobs/uploads/id", range: "0--1" } });
      }
      if (init?.method === "PUT") return new Response(null, { status: 201 });
      throw new Error(`unexpected request method ${init?.method}`);
    };

    await uploadBlobInChunks(
      "account/image",
      { digest: `sha256:${"b".repeat(64)}`, size: 4 },
      blobPath,
      { username: "user", password: "secret" },
      fetcher as typeof fetch,
      4,
      async (milliseconds) => { waits.push(milliseconds); },
    );

    expect({ heads, patches, statusChecks }).toEqual({ heads: 2, patches: 2, statusChecks: 2 });
    expect(waits).toEqual([250, 250]);
  });

  it("reports which registry operation exhausted its transport retries", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "deploy-blob-error-test-"));
    const blobPath = path.join(directory, "blob");
    await writeFile(blobPath, "abcd");
    let attempts = 0;
    const fetcher = async (): Promise<Response> => {
      attempts += 1;
      throw new TypeError("fetch failed");
    };
    await expect(uploadBlobInChunks(
      "account/image",
      { digest: `sha256:${"c".repeat(64)}`, size: 4 },
      blobPath,
      { username: "user", password: "secret" },
      fetcher as typeof fetch,
      4,
      async () => undefined,
    )).rejects.toThrow("Cloudflare registry blob check failed after 5 attempts: fetch failed");
    expect(attempts).toBe(5);
  });
});
