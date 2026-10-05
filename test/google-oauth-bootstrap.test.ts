import { chmod, lstat, mkdtemp, readFile, symlink, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { createConnection } from "node:net";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import {
  GOOGLE_AUTHORIZATION_URL,
  GOOGLE_DRIVE_SCOPE,
  GOOGLE_GMAIL_READONLY_SCOPE,
  GOOGLE_REVOKE_URL,
  GOOGLE_TOKEN_URL,
  GOOGLE_WORKSPACE_SCOPES,
  buildGoogleAuthorizationURL,
  exchangeAndVerifyGoogleAuthorization,
  exchangeGoogleAuthorizationCode,
  startLoopbackCallback,
  upsertSecretFile,
  verifyGoogleWorkspaceAccount,
} from "../tools/google-oauth-bootstrap.ts";

describe("Google OAuth bootstrap", () => {
  it("builds an offline installed-app request with state and PKCE", () => {
    const url = buildGoogleAuthorizationURL({
      clientId: "desktop-client",
      redirectUri: "http://127.0.0.1:12345",
      account: "agent@example.com",
      state: "state-value",
      codeChallenge: "challenge-value",
    });
    expect(url.origin + url.pathname).toBe(GOOGLE_AUTHORIZATION_URL);
    expect(Object.fromEntries(url.searchParams)).toMatchObject({
      client_id: "desktop-client",
      redirect_uri: "http://127.0.0.1:12345",
      response_type: "code",
      scope: GOOGLE_WORKSPACE_SCOPES.join(" "),
      access_type: "offline",
      prompt: "consent select_account",
      include_granted_scopes: "true",
      login_hint: "agent@example.com",
      state: "state-value",
      code_challenge: "challenge-value",
      code_challenge_method: "S256",
    });
  });

  it("exchanges a code with client_secret_post and validates the response", async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
      access_token: "access-token",
      refresh_token: "refresh-token",
      expires_in: 3600,
      token_type: "Bearer",
      scope: `openid ${GOOGLE_WORKSPACE_SCOPES.join(" ")}`,
    }), { status: 200, headers: { "content-type": "application/json" } }));
    await expect(exchangeGoogleAuthorizationCode({
      clientId: "client-id",
      clientSecret: "client-secret",
      code: "authorization-code",
      codeVerifier: "verifier",
      redirectUri: "http://127.0.0.1:12345",
    }, request)).resolves.toEqual({ accessToken: "access-token", refreshToken: "refresh-token", expiresIn: 3600 });

    expect(request).toHaveBeenCalledOnce();
    const [url, init] = request.mock.calls[0]!;
    expect(url).toBe(GOOGLE_TOKEN_URL);
    expect(init?.redirect).toBe("error");
    const body = init?.body as URLSearchParams;
    expect(Object.fromEntries(body)).toEqual({
      code: "authorization-code",
      client_id: "client-id",
      client_secret: "client-secret",
      redirect_uri: "http://127.0.0.1:12345",
      grant_type: "authorization_code",
      code_verifier: "verifier",
    });
  });

  it("rejects missing refresh tokens and does not expose response bodies", async () => {
    const missingRefresh = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
      access_token: "access-token",
      expires_in: 3600,
      token_type: "Bearer",
      scope: GOOGLE_WORKSPACE_SCOPES.join(" "),
    })));
    await expect(exchangeGoogleAuthorizationCode({
      clientId: "client", clientSecret: "secret", code: "code", codeVerifier: "verifier", redirectUri: "http://127.0.0.1:1",
    }, missingRefresh)).rejects.toThrow(/invalid refresh_token/);

    const providerSecret = "provider-secret-that-must-not-leak";
    const failed = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({ error: providerSecret }), { status: 400 }));
    const result = exchangeGoogleAuthorizationCode({
      clientId: "client", clientSecret: "secret", code: "code", codeVerifier: "verifier", redirectUri: "http://127.0.0.1:1",
    }, failed);
    await expect(result).rejects.toThrow(/HTTP 400/);
    await expect(result).rejects.not.toThrow(new RegExp(providerSecret));
  });

  it("rejects an authorization response missing a required Workspace scope", async () => {
    const request = vi.fn<typeof fetch>().mockResolvedValue(new Response(JSON.stringify({
      access_token: "access-token",
      refresh_token: "refresh-token",
      expires_in: 3600,
      token_type: "Bearer",
      scope: GOOGLE_DRIVE_SCOPE,
    })));
    const result = exchangeGoogleAuthorizationCode({
      clientId: "client", clientSecret: "secret", code: "code", codeVerifier: "verifier", redirectUri: "http://127.0.0.1:1",
    }, request);
    await expect(result).rejects.toThrow(GOOGLE_GMAIL_READONLY_SCOPE);
  });

  it("verifies the exact Drive account", async () => {
    const request = vi.fn<typeof fetch>().mockImplementation(async () => new Response(JSON.stringify({ user: { emailAddress: "Agent@Example.com" } })));
    await expect(verifyGoogleWorkspaceAccount("access-token", "agent@example.com", request)).resolves.toBe("Agent@Example.com");
    expect((request.mock.calls[0]![1]?.headers as Record<string, string>).authorization).toBe("Bearer access-token");
    await expect(verifyGoogleWorkspaceAccount("access-token", "other@example.com", request)).rejects.toThrow(/does not match/);
  });

  it("revokes the new refresh token when account verification fails", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const request = vi.fn<typeof fetch>(async (input, init) => {
      const url = String(input);
      calls.push({ url, init });
      if (url === GOOGLE_TOKEN_URL) return new Response(JSON.stringify({
        access_token: "access-token",
        refresh_token: "refresh-token",
        expires_in: 3600,
        token_type: "Bearer",
        scope: GOOGLE_WORKSPACE_SCOPES.join(" "),
      }));
      if (url.startsWith("https://www.googleapis.com/drive/v3/about")) {
        return new Response(JSON.stringify({ user: { emailAddress: "wrong@example.com" } }));
      }
      return new Response("", { status: 200 });
    });
    await expect(exchangeAndVerifyGoogleAuthorization({
      clientId: "client", clientSecret: "secret", code: "code", codeVerifier: "verifier",
      redirectUri: "http://127.0.0.1:1", expectedAccount: "agent@example.com",
    }, request)).rejects.toThrow(/does not match/);
    const revocation = calls.find((call) => call.url === GOOGLE_REVOKE_URL);
    expect(revocation).toBeDefined();
    expect((revocation!.init!.body as URLSearchParams).get("token")).toBe("refresh-token");
    expect(revocation!.url).not.toContain("refresh-token");
  });

  it("accepts one matching loopback callback and rejects state mismatch", async () => {
    const callback = await startLoopbackCallback("expected-state", 2_000);
    try {
      const response = await fetch(`${callback.redirectUri}/?state=expected-state&code=authorization-code`);
      expect(response.status).toBe(200);
      await expect(callback.waitForCode).resolves.toBe("authorization-code");
    } finally {
      await callback.close();
    }

    const wrongState = await startLoopbackCallback("expected-state", 2_000);
    try {
      const callbackError = wrongState.waitForCode.catch((error: unknown) => error);
      const response = await fetch(`${wrongState.redirectUri}/?state=wrong-state&code=authorization-code`);
      expect(response.status).toBe(400);
      await expect(callbackError).resolves.toEqual(expect.objectContaining({ message: expect.stringMatching(/state did not match/) }));
    } finally {
      await wrongState.close();
    }
  });

  it("closes the callback server when another client leaves a request open", async () => {
    const callback = await startLoopbackCallback("expected-state", 2_000);
    const endpoint = new URL(callback.redirectUri);
    const socket = createConnection({ host: endpoint.hostname, port: Number(endpoint.port) });
    try {
      await once(socket, "connect");
      socket.write(`GET / HTTP/1.1\r\nHost: ${endpoint.host}\r\n`);
      const response = await fetch(`${callback.redirectUri}/?state=expected-state&code=authorization-code`);
      expect(response.status).toBe(200);
      await expect(callback.waitForCode).resolves.toBe("authorization-code");
      await expect(callback.close()).resolves.toBeUndefined();
    } finally {
      socket.destroy();
      await callback.close();
    }
  });

  it("atomically updates one secret, preserves content, and sets mode 0600", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "google-oauth-secret-"));
    const file = path.join(root, ".env.secrets");
    await writeFile(file, "# Keep this comment\nOTHER=value\nGOOGLE_REFRESH=old\n", { mode: 0o644 });
    await upsertSecretFile(file, "GOOGLE_REFRESH", "new-refresh-token");
    expect(await readFile(file, "utf8")).toBe("# Keep this comment\nOTHER=value\nGOOGLE_REFRESH=new-refresh-token\n");
    expect((await lstat(file)).mode & 0o777).toBe(0o600);

    await upsertSecretFile(file, "SECOND_REFRESH", "another-token");
    expect(await readFile(file, "utf8")).toContain("SECOND_REFRESH=another-token\n");
  });

  it("rejects symlink and duplicate secret targets", async () => {
    const root = await mkdtemp(path.join(tmpdir(), "google-oauth-secret-"));
    const target = path.join(root, "target");
    const link = path.join(root, ".env.secrets");
    await writeFile(target, "GOOGLE_REFRESH=value\n");
    await symlink(target, link);
    await expect(upsertSecretFile(link, "GOOGLE_REFRESH", "new-token")).rejects.toThrow(/not a symlink/);
    await chmod(target, 0o600);

    const duplicate = path.join(root, "duplicate.env");
    await writeFile(duplicate, "GOOGLE_REFRESH=one\nexport GOOGLE_REFRESH=two\n");
    await expect(upsertSecretFile(duplicate, "GOOGLE_REFRESH", "three")).rejects.toThrow(/more than once/);
  });
});
