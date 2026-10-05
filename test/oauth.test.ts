import { describe, expect, it, vi } from "vitest";
import {
  OAuthTokenManager,
  type OAuthCredentialConfig,
  type OAuthPolicy,
} from "../src/oauth.ts";

const policy: OAuthPolicy = {
  tokenRequestTimeoutMs: 10_000,
  expirySkewMs: 30_000,
  maxTokenRequestBytes: 65_536,
  maxTokenResponseBytes: 65_536,
  maxTokenBytes: 16_384,
};

function clientCredential(overrides: Partial<OAuthCredentialConfig> = {}): OAuthCredentialConfig {
  return {
    tokenUrl: "https://auth.example.com/oauth/token",
    clientIdSecret: "CLIENT_ID",
    clientAuth: { method: "client_secret_basic", secret: "CLIENT_SECRET" },
    grant: { type: "client_credentials" },
    scopes: ["read", "write"],
    extraParameters: { audience: "https://api.example.com" },
    fallbackTtlMs: null,
    ...overrides,
  };
}

function tokenResponse(overrides: Record<string, unknown> = {}): Response {
  return Response.json({ access_token: "access-token", token_type: "Bearer", expires_in: 3600, ...overrides });
}

describe("OAuth token manager", () => {
  it("uses client_secret_basic and shares the cached token", async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(tokenResponse());
    const manager = new OAuthTokenManager(
      clientCredential(),
      policy,
      { CLIENT_ID: "client id", CLIENT_SECRET: "s:ecret" },
      request,
      () => 1_000,
    );

    const first = await manager.authorization();
    const second = await manager.authorization();

    expect(first).toMatchObject({ ok: true, authorization: "Bearer access-token", cache: "miss" });
    expect(second).toMatchObject({ ok: true, authorization: "Bearer access-token", cache: "hit" });
    expect(request).toHaveBeenCalledTimes(1);
    const [url, init] = request.mock.calls[0]!;
    expect(url).toBe("https://auth.example.com/oauth/token");
    expect(init?.redirect).toBe("manual");
    expect(new Headers(init?.headers).get("authorization")).toBe(
      `Basic ${Buffer.from("client+id:s%3Aecret", "utf8").toString("base64")}`,
    );
    expect(new URLSearchParams(init?.body as string)).toEqual(new URLSearchParams({
      audience: "https://api.example.com",
      grant_type: "client_credentials",
      scope: "read write",
    }));
  });

  it("uses body client authentication and reacquires after invalidation", async () => {
    const request = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(tokenResponse({ access_token: "first" }))
      .mockResolvedValueOnce(tokenResponse({ access_token: "second" }));
    const manager = new OAuthTokenManager(
      clientCredential({ clientAuth: { method: "client_secret_post", secret: "CLIENT_SECRET" } }),
      policy,
      { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      request,
    );

    const first = await manager.authorization();
    expect(first.ok).toBe(true);
    if (!first.ok) return;
    manager.invalidate("stale-generation");
    expect((await manager.authorization())).toMatchObject({ authorization: "Bearer first", cache: "hit" });
    manager.invalidate(first.generation);
    expect((await manager.authorization())).toMatchObject({ authorization: "Bearer second", cache: "miss" });
    const body = new URLSearchParams(request.mock.calls[0]![1]?.body as string);
    expect(body.get("client_id")).toBe("client");
    expect(body.get("client_secret")).toBe("secret");
    expect(new Headers(request.mock.calls[0]![1]?.headers).has("authorization")).toBe(false);
  });

  it("refreshes at the skewed expiry boundary", async () => {
    let now = 0;
    const request = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(tokenResponse({ access_token: "first", expires_in: 100 }))
      .mockResolvedValueOnce(tokenResponse({ access_token: "second", expires_in: 100 }));
    const manager = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" }, request, () => now,
    );

    expect(await manager.authorization()).toMatchObject({ authorization: "Bearer first", cache: "miss" });
    now = 89_999;
    expect(await manager.authorization()).toMatchObject({ authorization: "Bearer first", cache: "hit" });
    now = 90_000;
    expect(await manager.authorization()).toMatchObject({ authorization: "Bearer second", cache: "miss" });
  });

  it("drops a cached token when referenced Worker Secrets change", async () => {
    const environment: Record<string, unknown> = { CLIENT_ID: "client", CLIENT_SECRET: "first-secret" };
    const request = vi.fn<typeof fetch>()
      .mockResolvedValueOnce(tokenResponse({ access_token: "first" }))
      .mockResolvedValueOnce(tokenResponse({ access_token: "second" }));
    const manager = new OAuthTokenManager(clientCredential(), policy, environment, request);

    expect(await manager.authorization()).toMatchObject({ authorization: "Bearer first", cache: "miss" });
    environment.CLIENT_SECRET = "second-secret";
    expect(await manager.authorization()).toMatchObject({ authorization: "Bearer second", cache: "miss" });
    expect(request).toHaveBeenCalledTimes(2);
  });

  it("supports public-client refresh grants and a configured fallback lifetime", async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(tokenResponse({ expires_in: undefined }));
    const manager = new OAuthTokenManager(
      clientCredential({
        clientAuth: { method: "none" },
        grant: { type: "refresh_token", refreshTokenSecret: "REFRESH_TOKEN" },
        fallbackTtlMs: 60_000,
      }),
      policy,
      { CLIENT_ID: "public-client", REFRESH_TOKEN: "refresh-seed" },
      request,
    );

    expect(await manager.authorization()).toMatchObject({ ok: true, cache: "miss" });
    const body = new URLSearchParams(request.mock.calls[0]![1]?.body as string);
    expect(body.get("client_id")).toBe("public-client");
    expect(body.get("refresh_token")).toBe("refresh-seed");
    expect(body.get("grant_type")).toBe("refresh_token");
  });

  it("coalesces concurrent cache misses into one token request", async () => {
    let resolve!: (response: Response) => void;
    const pending = new Promise<Response>((done) => { resolve = done; });
    const request = vi.fn<typeof fetch>().mockReturnValue(pending);
    const manager = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" }, request,
    );

    const first = manager.authorization();
    const second = manager.authorization();
    await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(1));
    resolve(tokenResponse());
    expect(await first).toMatchObject({ ok: true });
    expect(await second).toMatchObject({ ok: true });
  });

  it("fails closed on refresh-token rotation", async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(tokenResponse({ refresh_token: "rotated" }));
    const manager = new OAuthTokenManager(
      clientCredential({ grant: { type: "refresh_token", refreshTokenSecret: "REFRESH_TOKEN" } }),
      policy,
      { CLIENT_ID: "client", CLIENT_SECRET: "secret", REFRESH_TOKEN: "seed" },
      request,
    );

    expect(await manager.authorization()).toMatchObject({
      ok: false,
      status: 502,
      code: "oauth_refresh_token_rotation_unsupported",
    });
  });

  it("rejects missing expiry, unsupported token types, and oversized responses", async () => {
    const missingExpiry = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockResolvedValue(tokenResponse({ expires_in: undefined })),
    );
    expect(await missingExpiry.authorization()).toMatchObject({ code: "oauth_invalid_token_response" });

    const dpop = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockResolvedValue(tokenResponse({ token_type: "DPoP" })),
    );
    expect(await dpop.authorization()).toMatchObject({ code: "oauth_unsupported_token_type" });

    const oversized = new OAuthTokenManager(
      clientCredential(), { ...policy, maxTokenResponseBytes: 1024 }, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockResolvedValue(new Response("x".repeat(1025))),
    );
    expect(await oversized.authorization()).toMatchObject({ code: "oauth_invalid_token_response" });
  });

  it("maps endpoint rejection and unavailability without returning response bodies", async () => {
    const rejected = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockResolvedValue(new Response("sensitive", { status: 400 })),
    );
    expect(await rejected.authorization()).toEqual({
      ok: false,
      status: 502,
      code: "oauth_token_rejected",
      message: "OAuth token endpoint rejected the configured grant",
      upstreamStatus: 400,
    });

    const redirect = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 302, headers: { location: "https://other.example/token" } })),
    );
    expect(await redirect.authorization()).toEqual({
      ok: false,
      status: 502,
      code: "oauth_token_rejected",
      message: "OAuth token endpoint rejected the configured grant",
      upstreamStatus: 302,
    });

    const unavailable = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" },
      vi.fn<typeof fetch>().mockRejectedValue(new Error("sensitive network error")),
    );
    const logged = vi.spyOn(console, "error").mockImplementation(() => undefined);
    try {
      expect(await unavailable.authorization()).toEqual({
        ok: false,
        status: 503,
        code: "oauth_token_unavailable",
        message: "OAuth token endpoint is unavailable",
      });
      expect(logged).toHaveBeenCalledWith(expect.stringContaining('"event":"oauth_token_fetch_failed"'));
      expect(logged).toHaveBeenCalledWith(expect.stringContaining('"error_message":"sensitive network error"'));
      expect(logged).not.toHaveBeenCalledWith(expect.stringContaining('"client"'));
    } finally {
      logged.mockRestore();
    }
  });

  it("invokes the fetch implementation without binding the token manager as this", async () => {
    const request = vi.fn(function (this: unknown) {
      if (this !== undefined) throw new TypeError("Illegal invocation");
      return Promise.resolve(tokenResponse());
    });
    const manager = new OAuthTokenManager(
      clientCredential(), policy, { CLIENT_ID: "client", CLIENT_SECRET: "secret" }, request,
    );

    await expect(manager.authorization()).resolves.toMatchObject({ ok: true, cache: "miss" });
  });
});
