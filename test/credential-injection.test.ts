import { afterEach, describe, expect, it, vi } from "vitest";
import { containerProxyToken, credentialTarget, injectCredential, type CredentialInjectionRule } from "../src/credential-proxy.ts";
import { appConfig } from "../src/generated-config.ts";

const runnerSecret = "r".repeat(48);
const containerId = "container-1";

function basic(value: string): string {
  return `Basic ${Buffer.from(value, "utf8").toString("base64")}`;
}

const credentialInjections: CredentialInjectionRule[] = [
  {
    targetKind: "route",
    targetName: "github",
    credentialName: "github",
    urlPrefix: "https://api.github.com",
    sourceHeader: "authorization",
    sourceValuePrefix: "token ",
    upstream: { kind: "static", header: "authorization", secret: "GH_TOKEN", valuePrefix: "token " },
  },
  {
    credentialName: "linear",
    urlPrefix: "https://api.linear.app",
    sourceHeader: "authorization",
    sourceValuePrefix: "",
    upstream: { kind: "static", header: "authorization", secret: "LINEAR_API_KEY", valuePrefix: "" },
  },
  {
    credentialName: "close",
    urlPrefix: "https://api.close.com/api/v1",
    sourceHeader: "authorization",
    sourceValuePrefix: "Basic ",
    upstream: { kind: "static", header: "authorization", secret: "CLOSE_API_KEY", valuePrefix: "Basic " },
  },
  {
    credentialName: "password",
    urlPrefix: "https://password.example.com/v1",
    sourceHeader: "authorization",
    sourceValuePrefix: "Basic ",
    upstream: { kind: "static", header: "authorization", secret: "PASSWORD_API_CREDENTIAL", valuePrefix: "Basic " },
  },
  ...appConfig.credentialInjections.filter((rule) => rule.credentialName === "anthropic"),
];

const oauthRule: CredentialInjectionRule = {
  targetKind: "route",
  targetName: "oauth-api",
  credentialName: "oauth-api",
  urlPrefix: "https://oauth-api.example.com/v1",
  sourceHeader: "authorization",
  sourceValuePrefix: "Bearer ",
  upstream: { kind: "oauth" },
};

function environment(): Record<string, unknown> {
  return {
    RUNNER_API_TOKEN: runnerSecret,
    ANTHROPIC_API_KEY: "real-anthropic-credential",
    GH_TOKEN: "real-github-credential",
    LINEAR_API_KEY: "real-linear-credential",
    CLOSE_API_KEY: "real-close-key:",
    PASSWORD_API_CREDENTIAL: "service-user:pässword:with-colon",
  };
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("outbound credential injection", () => {
  it("classifies configured egress without returning credential metadata", () => {
    expect(credentialTarget(
      new Request("https://api.github.com/repos/example/project"),
      credentialInjections,
    )).toEqual({ kind: "route", name: "github" });
    expect(credentialTarget(
      new Request("https://unconfigured.example.com"),
      credentialInjections,
    )).toBeNull();
  });

  it("accepts GitHub CLI's token scheme and replaces it upstream", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    const response = await injectCredential(new Request("https://api.github.com/repos/example/project", {
      headers: { authorization: `token ${token}` },
    }), environment(), containerId, credentialInjections);

    expect(response.status).toBe(200);
    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBe("token real-github-credential");
  });

  it("accepts and replaces Linear's raw authorization value", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    await injectCredential(new Request("https://api.linear.app/graphql", {
      headers: { authorization: token },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBe("real-linear-credential");
  });

  it("replaces a scoped Basic username with Close's complete raw credential", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    await injectCredential(new Request("https://api.close.com/api/v1/opportunity/", {
      headers: { authorization: basic(`${token}:`) },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBe(basic("real-close-key:"));
  });

  it("accepts an empty-password Basic credential normalized to the bare scoped token", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    await injectCredential(new Request("https://api.close.com/api/v1/me/", {
      headers: { authorization: basic(token) },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBe(basic("real-close-key:"));
  });

  it("accepts the scoped token as a Basic password and emits UTF-8 credentials", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);
    const presented = basic(`client-selected-user:${token}`).replace(/^Basic /u, "bAsIc   ");

    await injectCredential(new Request("https://password.example.com/v1/data", {
      headers: { authorization: presented },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBe(basic("service-user:pässword:with-colon"));
  });

  it("removes duplicate scoped credential headers before upstream injection", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const rule = appConfig.credentialInjections.find((candidate) => candidate.credentialName === "anthropic");
    expect(rule).toBeDefined();
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    await injectCredential(new Request(`${rule!.urlPrefix}/messages`, {
      headers: { authorization: `Bearer ${token}`, "x-api-key": token },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("authorization")).toBeNull();
    expect(request.headers.get("x-api-key")).toBe("real-anthropic-credential");
  });

  it("rejects invalid credentials and scoped credentials sent to other hosts", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unexpected"));

    const invalid = await injectCredential(new Request("https://api.github.com/repos/example/project", {
      headers: { authorization: "token invalid" },
    }), environment(), containerId, credentialInjections);
    expect(invalid.status).toBe(401);

    const leaked = await injectCredential(new Request("https://example.com/collect", {
      headers: { authorization: `token ${token}` },
    }), environment(), containerId, credentialInjections);
    expect(leaked.status).toBe(403);
    expect(upstream).not.toHaveBeenCalled();
  });

  it("rejects malformed Basic credentials and partial scoped-token matches", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unexpected"));
    const requests = [
      new Request("https://api.close.com/api/v1/me/", { headers: { authorization: "Basic !!!" } }),
      new Request("https://api.close.com/api/v1/me/", { headers: { authorization: basic(`${token}-suffix:`) } }),
      new Request("https://api.close.com/api/v1/me/", { headers: { authorization: basic(`${token}:\u0007`) } }),
      new Request("https://api.close.com/api/v1/me/", { headers: { authorization: `Basic ${Buffer.from([0xc3, 0x28, 0x3a]).toString("base64")}` } }),
    ];

    for (const request of requests) {
      const response = await injectCredential(request, environment(), containerId, credentialInjections);
      expect(response.status).toBe(401);
    }
    expect(upstream).not.toHaveBeenCalled();
  });

  it("blocks a Base64-encoded scoped token from leaking to an unconfigured URL", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unexpected"));

    const response = await injectCredential(new Request("https://example.com/collect", {
      headers: { authorization: basic(`service:${token}`) },
    }), environment(), containerId, credentialInjections);

    expect(response.status).toBe(403);
    expect(upstream).not.toHaveBeenCalled();
  });

  it("removes duplicate Basic scoped credentials before upstream injection", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("ok"));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    await injectCredential(new Request("https://api.close.com/api/v1/me/", {
      headers: {
        authorization: basic(`${token}:`),
        "x-forwarded-credential": basic(`service:${token}`),
      },
    }), environment(), containerId, credentialInjections);

    const request = upstream.mock.calls[0]![0] as Request;
    expect(request.headers.get("x-forwarded-credential")).toBeNull();
  });

  it("rejects malformed upstream Basic secrets without making a request", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unexpected"));
    const invalidEnvironment = { ...environment(), CLOSE_API_KEY: "missing-delimiter" };

    const response = await injectCredential(new Request("https://api.close.com/api/v1/me/", {
      headers: { authorization: basic(`${token}:`) },
    }), invalidEnvironment, containerId, credentialInjections);

    expect(response.status).toBe(500);
    expect(upstream).not.toHaveBeenCalled();
  });

  it("injects brokered OAuth authorization and invalidates a rejected generation without replay", async () => {
    const token = await containerProxyToken(runnerSecret, containerId);
    const broker = {
      authorization: vi.fn(async () => ({
        ok: true as const,
        authorization: "Bearer oauth-access-token",
        generation: "generation-1",
        cache: "miss" as const,
      })),
      invalidate: vi.fn(async () => undefined),
    };
    const namespace = {
      idFromName: vi.fn((name: string) => name),
      get: vi.fn(() => broker),
    };
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unauthorized", { status: 401 }));
    vi.spyOn(console, "log").mockImplementation(() => undefined);

    const response = await injectCredential(new Request("https://oauth-api.example.com/v1/items", {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, cookie: "private=value" },
      body: "payload",
    }), { ...environment(), OAUTH_CREDENTIAL_BROKER: namespace }, containerId, [...credentialInjections, oauthRule]);

    expect(response.status).toBe(401);
    expect(upstream).toHaveBeenCalledTimes(1);
    const forwarded = upstream.mock.calls[0]![0] as Request;
    expect(forwarded.headers.get("authorization")).toBe("Bearer oauth-access-token");
    expect(forwarded.headers.get("cookie")).toBeNull();
    expect(broker.invalidate).toHaveBeenCalledWith("oauth-api", "generation-1");
  });

  it("does not acquire OAuth tokens before validating the scoped credential", async () => {
    const broker = {
      authorization: vi.fn(async () => ({ ok: false as const, status: 503 as const, code: "unexpected", message: "unexpected" })),
      invalidate: vi.fn(async () => undefined),
    };
    const namespace = { idFromName: vi.fn((name: string) => name), get: vi.fn(() => broker) };
    const upstream = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("unexpected"));

    const response = await injectCredential(new Request("https://oauth-api.example.com/v1/items", {
      headers: { authorization: "Bearer invalid" },
    }), { ...environment(), OAUTH_CREDENTIAL_BROKER: namespace }, containerId, [...credentialInjections, oauthRule]);

    expect(response.status).toBe(401);
    expect(broker.authorization).not.toHaveBeenCalled();
    expect(upstream).not.toHaveBeenCalled();
  });
});
