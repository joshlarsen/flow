import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { stringify } from "yaml";
import {
  isEnvironmentProfile,
  type EnvironmentProfile,
} from "./environment-files.ts";
import {
  loadConfigurationEnvironments,
  loadRunnerConfig,
  toRuntimeConfig,
} from "./config.ts";

const slackAppNameLimit = 35;

export interface SlackAppManifest {
  display_information: {
    name: string;
    description: string;
  };
  features: {
    bot_user: {
      display_name: string;
      always_online: false;
    };
  };
  oauth_config: {
    scopes: {
      bot: ["chat:write"];
    };
  };
  settings: {
    interactivity: {
      is_enabled: true;
      request_url: string;
    };
    org_deploy_enabled: false;
    socket_mode_enabled: false;
    token_rotation_enabled: false;
  };
}

function slackAppName(deploymentName: string): string {
  const suffix = deploymentName.replace(/[-_]+/g, " ").replace(/\s+/g, " ").trim();
  const candidate = suffix ? `Agent Runner - ${suffix}` : "Agent Runner";
  return candidate.slice(0, slackAppNameLimit).trimEnd();
}

/** Builds a secret-free Slack app manifest for one deployed Worker origin. */
export function createSlackAppManifest(runnerUrl: string, deploymentName: string): SlackAppManifest {
  let parsed: URL;
  try {
    parsed = new URL(runnerUrl);
  } catch {
    throw new Error("RUNNER_URL must be a public HTTPS origin without credentials, a path, a query, or a fragment");
  }
  const loopback = parsed.hostname === "localhost"
    || parsed.hostname === "127.0.0.1"
    || parsed.hostname === "[::1]";
  if (
    parsed.protocol !== "https:"
    || loopback
    || parsed.username
    || parsed.password
    || parsed.pathname !== "/"
    || parsed.search
    || parsed.hash
  ) {
    throw new Error("RUNNER_URL must be a public HTTPS origin without credentials, a path, a query, or a fragment");
  }

  return {
    display_information: {
      name: slackAppName(deploymentName),
      description: "Delivers agent input requests and resumes waiting workflows.",
    },
    features: {
      bot_user: {
        display_name: "Agent Runner",
        always_online: false,
      },
    },
    oauth_config: {
      scopes: {
        bot: ["chat:write"],
      },
    },
    settings: {
      interactivity: {
        is_enabled: true,
        request_url: `${parsed.origin}/v1/interactions/slack`,
      },
      org_deploy_enabled: false,
      socket_mode_enabled: false,
      token_rotation_enabled: false,
    },
  };
}

/** Writes the Slack app manifest for the active environment profile. */
export async function generateSlackAppManifest(
  root = process.cwd(),
  profile?: EnvironmentProfile,
): Promise<string> {
  const [config, environments] = await Promise.all([
    loadRunnerConfig(root),
    loadConfigurationEnvironments(root, profile),
  ]);
  const runtime = toRuntimeConfig(config, environments.cloudflare, environments.files.profile);
  if (!runtime.runnerUrl) {
    throw new Error("Slack manifest generation requires RUNNER_URL in the active profile's .env.cloudflare");
  }

  const generatedDir = path.join(root, ".generated");
  const outputPath = path.join(generatedDir, "slack-app-manifest.yaml");
  await mkdir(generatedDir, { recursive: true });
  await writeFile(
    outputPath,
    stringify(createSlackAppManifest(runtime.runnerUrl, runtime.deploymentName), { lineWidth: 0 }),
  );
  return outputPath;
}

function requestedProfile(argv: string[]): EnvironmentProfile | undefined {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  if (args.length === 0) return undefined;
  if (args.length === 2 && args[0] === "--profile" && isEnvironmentProfile(args[1] ?? "")) {
    return args[1] as EnvironmentProfile;
  }
  throw new Error("Usage: pnpm slack:manifest [-- --profile local|prod]");
}

async function main(): Promise<void> {
  const outputPath = await generateSlackAppManifest(
    process.cwd(),
    requestedProfile(process.argv.slice(2)),
  );
  process.stdout.write(`Generated ${path.relative(process.cwd(), outputPath)}\n`);
}

if (
  process.argv[1]
  && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)
) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
