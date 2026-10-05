import { describe, expect, it } from "vitest";
import {
  assertLocalDevelopmentProfile,
  devWranglerArguments,
  ensureDevTunnel,
  resolveDevTunnelName,
  tunnelCreateArguments,
  tunnelInfoArguments,
} from "../tools/dev.ts";

describe("local development tunnel", () => {
  it("allows only the explicitly active local profile", () => {
    expect(() => assertLocalDevelopmentProfile("local")).not.toThrow();
    expect(() => assertLocalDevelopmentProfile("prod")).toThrow(/requires the active local environment profile/);
    expect(() => assertLocalDevelopmentProfile(null)).toThrow(/requires the active local environment profile/);
  });

  it("requires a configured named tunnel", () => {
    expect(resolveDevTunnelName({ CLOUDFLARE_TUNNEL_NAME: " runner-dev " })).toBe("runner-dev");
    expect(() => resolveDevTunnelName({})).toThrow(/CLOUDFLARE_TUNNEL_NAME.*named tunnel/);
  });

  it("always enables the named tunnel and preserves extra Wrangler arguments", () => {
    expect(devWranglerArguments("runner-dev", ["--", "--port", "8788"])).toEqual([
      "dev",
      "--tunnel",
      "--tunnel-name", "runner-dev",
      "--env-file", ".env.cloudflare",
      "--env-file", ".env.secrets",
      "--env-file", ".env",
      "-c", ".generated/wrangler.jsonc",
      "--port", "8788",
    ]);
  });

  it("pins the local origin port when none is provided", () => {
    expect(devWranglerArguments("runner-dev")).toContain("8787");
    expect(devWranglerArguments("runner-dev", ["--port=8788"])).not.toContain("8787");
  });

  it("leaves an existing tunnel unchanged", async () => {
    let createCalled = false;
    const created = await ensureDevTunnel(
      "runner-dev",
      {
        captureCommand: async (command, args) => {
          expect([command, ...args]).toEqual(["wrangler", ...tunnelInfoArguments("runner-dev")]);
          return { code: 0, stdout: "Tunnel Information", stderr: "" };
        },
        runCommand: async () => {
          createCalled = true;
          return 0;
        },
      },
    );
    expect(created).toBe(false);
    expect(createCalled).toBe(false);
  });

  it("creates a tunnel when Wrangler confirms that it is missing", async () => {
    const messages: string[] = [];
    const created = await ensureDevTunnel(
      "runner-dev",
      {
        captureCommand: async () => ({
          code: 1,
          stdout: "",
          stderr: '"runner-dev" is neither the ID nor the name of any of your tunnels',
        }),
        runCommand: async (command, args) => {
          expect([command, ...args]).toEqual(["wrangler", ...tunnelCreateArguments("runner-dev")]);
          return 0;
        },
        info: (message) => messages.push(message),
      },
    );
    expect(created).toBe(true);
    expect(messages).toEqual(['Cloudflare Tunnel "runner-dev" does not exist; creating it.']);
  });

  it("does not create a tunnel when inspection fails for another reason", async () => {
    await expect(ensureDevTunnel(
      "runner-dev",
      {
        captureCommand: async () => ({ code: 1, stdout: "", stderr: "Authentication failed" }),
        runCommand: async () => 0,
      },
    )).rejects.toThrow(/Unable to inspect.*Authentication failed/s);
  });

  it("passes active profile files to Wrangler", () => {
    expect(devWranglerArguments("runner-dev", [], {
      runtime: "/repo/.env",
      secrets: "/repo/.env.profiles/local/.env.secrets",
      cloudflare: "/repo/.env.profiles/local/.env.cloudflare",
    })).toEqual(expect.arrayContaining([
      "--env-file", "/repo/.env.profiles/local/.env.cloudflare",
      "--env-file", "/repo/.env.profiles/local/.env.secrets",
      "--env-file", "/repo/.env",
    ]));
  });
});
