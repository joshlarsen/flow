import { lstat, readFile } from "node:fs/promises";
import path from "node:path";

export const environmentProfiles = ["local", "prod"] as const;
export type EnvironmentProfile = (typeof environmentProfiles)[number];

export interface ConfigurationEnvironmentFiles {
  profile: EnvironmentProfile | null;
  runtime: string;
  secrets: string;
  cloudflare: string;
  marker: string;
}

export function isEnvironmentProfile(
  value: string,
): value is EnvironmentProfile {
  return environmentProfiles.includes(value as EnvironmentProfile);
}

/** Returns the source files for a named environment profile. */
export function profileEnvironmentFiles(
  root: string,
  profile: EnvironmentProfile,
): ConfigurationEnvironmentFiles {
  const directory = path.join(root, ".env.profiles", profile);
  return {
    profile,
    runtime: path.join(root, ".env"),
    secrets: path.join(directory, ".env.secrets"),
    cloudflare: path.join(directory, ".env.cloudflare"),
    marker: path.join(root, ".env.active"),
  };
}

/** Returns empty profile paths for offline validation before profile selection. */
export function unselectedEnvironmentFiles(
  root: string,
): ConfigurationEnvironmentFiles {
  return {
    profile: null,
    runtime: path.join(root, ".env"),
    secrets: path.join(root, ".env.profiles", "unselected", ".env.secrets"),
    cloudflare: path.join(
      root,
      ".env.profiles",
      "unselected",
      ".env.cloudflare",
    ),
    marker: path.join(root, ".env.active"),
  };
}

/** Reads and validates the active-profile marker without following symlinks. */
export async function readActiveEnvironmentProfile(
  root: string,
): Promise<EnvironmentProfile | null> {
  const marker = path.join(root, ".env.active");
  try {
    const metadata = await lstat(marker);
    if (!metadata.isFile() || metadata.isSymbolicLink())
      throw new Error(`${marker} must be a regular file`);
    if (metadata.size > 32) throw new Error(`${marker} is too large`);
    const value = (await readFile(marker, "utf8")).trim();
    if (!isEnvironmentProfile(value))
      throw new Error(`${marker} must contain local or prod`);
    return value;
  } catch (error) {
    if (
      error &&
      typeof error === "object" &&
      "code" in error &&
      error.code === "ENOENT"
    )
      return null;
    throw error;
  }
}

/** Resolves either an explicitly requested profile or the currently active environment files. */
export async function resolveEnvironmentFiles(
  root: string,
  requestedProfile?: EnvironmentProfile,
): Promise<ConfigurationEnvironmentFiles> {
  const profile =
    requestedProfile ?? (await readActiveEnvironmentProfile(root));
  return profile
    ? profileEnvironmentFiles(root, profile)
    : unselectedEnvironmentFiles(root);
}

/** Requires any explicit profile before a command may mutate remote state. */
export function requireActiveEnvironmentProfile(
  profile: EnvironmentProfile | null,
  command: string,
): EnvironmentProfile {
  if (!profile) {
    throw new Error(
      `${command} requires an active environment profile; run pnpm env:local or pnpm env:prod`,
    );
  }
  return profile;
}

/** Requires one specific profile before a profile-constrained command continues. */
export function requireEnvironmentProfile<T extends EnvironmentProfile>(
  profile: EnvironmentProfile | null,
  expected: T,
  command: string,
): T {
  if (profile !== expected) {
    throw new Error(
      `${command} requires the active ${expected} environment profile; run pnpm env:${expected}`,
    );
  }
  return expected;
}
