import { describe, expect, it } from "vitest";
import {
  classifyProbe,
  parseProbeOptions,
  pinnedACPVersionFromDockerfile,
  probeExitCode,
  type CommandResult,
} from "../tools/claude-amd64-probe.ts";

const commandResult = (overrides: Partial<CommandResult>): CommandResult => ({
  code: 0,
  signal: null,
  stdout: "2.1.274 (Claude Code)\n",
  stderr: "",
  timedOut: false,
  ...overrides,
});

describe("Claude amd64 probe classification", () => {
  it("recognizes successful and known Bun crash outcomes", () => {
    expect(classifyProbe(commandResult({}))).toEqual({ status: "compatible", detail: "2.1.274 (Claude Code)" });
    expect(classifyProbe(commandResult({ code: 139, stdout: "", stderr: "qemu: uncaught target signal 11 (Segmentation fault)" })).status)
      .toBe("known_incompatible");
    expect(classifyProbe(commandResult({ code: 134, stdout: "", stderr: "ASSERTION FAILED: MemoryExhaustion\nBun has crashed" })).status)
      .toBe("known_incompatible");
  });

  it("keeps timeouts and unrelated failures distinct", () => {
    expect(classifyProbe(commandResult({ code: null, timedOut: true, stdout: "" })).status).toBe("error");
    expect(classifyProbe(commandResult({ code: 2, stdout: "", stderr: "could not locate executable" })).status).toBe("error");
  });
});

describe("Claude amd64 probe exit status", () => {
  it("distinguishes compatibility, the known emulation failure, and probe errors", () => {
    expect(probeExitCode({ status: "compatible", detail: "ok" })).toBe(0);
    expect(probeExitCode({ status: "known_incompatible", detail: "Bun crash" })).toBe(1);
    expect(probeExitCode({ status: "error", detail: "setup failed" })).toBe(2);
  });
});

describe("Claude amd64 probe arguments", () => {
  it("defaults to the Dockerfile pin and accepts explicit release selectors", () => {
    expect(parseProbeOptions([], "0.79.0")).toEqual({ acpVersion: "0.79.0" });
    expect(parseProbeOptions(["--", "--acp-version", "latest"], "0.79.0")).toEqual({ acpVersion: "latest" });
    expect(pinnedACPVersionFromDockerfile("FROM node:24\nARG CLAUDE_ACP_VERSION=0.79.0\n")).toBe("0.79.0");
  });

  it("rejects standalone-Claude flags and arbitrary npm specifications", () => {
    expect(() => parseProbeOptions(["--claude-version", "latest"], "0.79.0")).toThrow(/Unknown argument/);
    expect(() => parseProbeOptions(["--acp-version", "github:user/repo"], "0.79.0")).toThrow(/requires a semver/);
    expect(() => pinnedACPVersionFromDockerfile("ARG CLAUDE_ACP_VERSION=github:user/repo\n")).toThrow(/valid/);
  });
});
