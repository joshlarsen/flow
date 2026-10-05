import { describe, expect, it } from "vitest";
import { createSlackAppManifest } from "../tools/generate-slack-manifest.ts";

describe("Slack app manifest", () => {
  it("configures the bot scope and public interaction callback", () => {
    const manifest = createSlackAppManifest(
      "https://agent-runner.example.com",
      "agent-runner-prod",
    );

    expect(manifest.display_information.name).toBe("Agent Runner - agent runner prod");
    expect(manifest.features.bot_user).toEqual({
      display_name: "Agent Runner",
      always_online: false,
    });
    expect(manifest.oauth_config.scopes.bot).toEqual(["chat:write"]);
    expect(manifest.settings.interactivity).toEqual({
      is_enabled: true,
      request_url: "https://agent-runner.example.com/v1/interactions/slack",
    });
    expect(JSON.stringify(manifest)).not.toMatch(/SLACK_(BOT_TOKEN|SIGNING_SECRET)|xox[baprs]-/);
  });

  it("rejects origins Slack cannot call", () => {
    expect(() => createSlackAppManifest("http://localhost:8787", "runner")).toThrow(/public HTTPS origin/);
    expect(() => createSlackAppManifest("https://localhost:8787", "runner")).toThrow(/public HTTPS origin/);
    expect(() => createSlackAppManifest("https://runner.example.com/path", "runner")).toThrow(/public HTTPS origin/);
    expect(() => createSlackAppManifest("not a URL", "runner")).toThrow(/public HTTPS origin/);
  });

  it("keeps generated app names within Slack's limit", () => {
    const manifest = createSlackAppManifest(
      "https://runner.example.com",
      "agent-runner-with-an-extremely-long-deployment-name",
    );
    expect(manifest.display_information.name.length).toBeLessThanOrEqual(35);
  });
});
