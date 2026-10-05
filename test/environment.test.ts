import { mkdir, mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { describe, expect, it } from "vitest";
import {
  activateEnvironmentProfile,
  ensureEnvironmentProfile,
  runEnvironmentCommand,
} from "../tools/environment.ts";
import {
  readActiveEnvironmentProfile,
  requireActiveEnvironmentProfile,
  resolveEnvironmentFiles,
} from "../tools/environment-files.ts";

async function temporaryRoot(): Promise<string> {
  const root=await mkdtemp(path.join(tmpdir(), "runner-environment-"));
  await mkdir(path.join(root,"config"));
  await writeFile(path.join(root,"config/env.example"),"# runtime\n");
  await Promise.all([writeFile(path.join(root,"config/env.secrets.example"),"LOCAL_SECRET=secret\n"),writeFile(path.join(root,"config/env.local.cloudflare.example"),"LOCAL_SETTING=value\n"),writeFile(path.join(root,"config/env.prod.cloudflare.example"),"PROD_SETTING=value\n")]);
  return root;
}

async function writeProfile(root: string, profile: "local" | "prod"): Promise<void> {
  const directory = path.join(root, ".env.profiles", profile);
  await mkdir(directory, { recursive: true });
  await Promise.all([
    writeFile(path.join(directory, ".env.secrets"), `PROFILE_SECRET=${profile}\n`),
    writeFile(path.join(directory, ".env.cloudflare"), `PROFILE_NAME=${profile}\n`),
  ]);
}

describe("environment profiles", () => {
  it("requires an explicit profile before remote mutation", () => {
    expect(requireActiveEnvironmentProfile("local", "pnpm bundle:sync")).toBe("local");
    expect(requireActiveEnvironmentProfile("prod", "pnpm bundle:sync")).toBe("prod");
    expect(() => requireActiveEnvironmentProfile(null, "pnpm bundle:sync")).toThrow(/requires an active environment profile/);
  });

  it("initializes local from examples without importing root files and preserves secret permissions", async () => {
    const root = await temporaryRoot();
    await Promise.all([
      writeFile(path.join(root, ".env.secrets"), "LOCAL_SECRET=secret\n"),
      writeFile(path.join(root, ".env.cloudflare"), "LOCAL_SETTING=value\n"),
    ]);

    await ensureEnvironmentProfile(root, "local");
    const files = await resolveEnvironmentFiles(root, "local");
    expect(await readFile(files.secrets, "utf8")).toBe("LOCAL_SECRET=secret\n");
    expect(await readFile(files.cloudflare, "utf8")).toBe("LOCAL_SETTING=value\n");
    expect((await stat(files.secrets)).mode & 0o777).toBe(0o600);
  });

  it("initializes production from its own example", async () => {
    const root = await temporaryRoot();
    await ensureEnvironmentProfile(root,"prod");
    expect(await readFile((await resolveEnvironmentFiles(root,"prod")).cloudflare,"utf8")).toBe("PROD_SETTING=value\n");
  });

  it("activates a profile atomically and resolves its source files", async () => {
    const root = await temporaryRoot();
    await writeProfile(root, "local");
    await activateEnvironmentProfile(root, "local");
    expect(await readActiveEnvironmentProfile(root)).toBe("local");
    expect((await resolveEnvironmentFiles(root)).secrets).toBe(path.join(root, ".env.profiles", "local", ".env.secrets"));
  });

  it("runs production secret synchronization before deployment", async () => {
    const root = await temporaryRoot();
    await writeProfile(root, "prod");
    const commands: string[][] = [];
    const messages: string[] = [];
    await runEnvironmentCommand("prod", root, {
      validate: async () => undefined,
      run: async (command, args) => {
        commands.push([command, ...args]);
        return 0;
      },
      output: (message) => messages.push(message),
    });
    expect(await readActiveEnvironmentProfile(root)).toBe("prod");
    expect(commands).toEqual([
      ["pnpm", "secrets:sync"],
      ["pnpm", "run", "deploy"],
    ]);
    expect(messages.at(-1)).toMatch(/synchronized/);
  });

  it("stops production deployment when secret synchronization fails", async () => {
    const root = await temporaryRoot();
    await writeProfile(root, "prod");
    const commands: string[][] = [];
    await expect(runEnvironmentCommand("prod", root, {
      validate: async () => undefined,
      run: async (command, args) => {
        commands.push([command, ...args]);
        return 2;
      },
      output: () => undefined,
    })).rejects.toThrow(/secret synchronization failed/);
    expect(commands).toEqual([["pnpm", "secrets:sync"]]);
    expect(await readActiveEnvironmentProfile(root)).toBe("prod");
  });

  it("reports profile paths without reading or displaying values", async () => {
    const root = await temporaryRoot();
    await writeProfile(root, "local");
    await activateEnvironmentProfile(root, "local");
    const messages: string[] = [];
    await runEnvironmentCommand("status", root, { output: (message) => messages.push(message) });
    expect(messages.join("\n")).toContain("Environment: local");
    expect(messages.join("\n")).toContain(".env.profiles/local/.env.secrets");
    expect(messages.join("\n")).not.toContain("PROFILE_SECRET");
  });
});
