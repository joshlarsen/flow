/** Resolves every declared plaintext Worker binding for the container runtime. */
export function runtimeEnvironment(
  environment: Record<string, unknown>,
  names: readonly string[],
): Record<string, string> {
  const values: Record<string, string> = {};
  for (const name of names) {
    const value = environment[name];
    if (typeof value !== "string" || !value.trim()) {
      throw new Error(`plaintext environment variable ${name} is not configured`);
    }
    values[name] = value;
  }
  return values;
}
