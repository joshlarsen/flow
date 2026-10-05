const maxResponseBytes = 16 * 1024;

/** Reads a bounded JSON response from the runner. */
async function readBoundedJSON(response: Response): Promise<unknown> {
  if (!response.body) return null;
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > maxResponseBytes) {
      await reader.cancel();
      throw new Error("Runner preflight response exceeded 16384 bytes");
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    return null;
  }
}

/** Requires the deployed runner to verify its deployed configuration before a run starts. */
export async function requireRunnerPreflight(
  runnerURL: URL,
  runnerToken: string,
  request: typeof fetch = fetch,
): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await request(new URL("/v1/preflight", runnerURL), {
      headers: { authorization: `Bearer ${runnerToken}` },
      signal: controller.signal,
    });
    const body = await readBoundedJSON(response);
    if (!response.ok) {
      throw new Error(`Runner preflight failed with HTTP ${response.status}`);
    }
    if (
      !body ||
      typeof body !== "object" ||
      Array.isArray(body) ||
      (body as Record<string, unknown>).status !== "ready"
    ) {
      throw new Error("Runner preflight returned an invalid readiness response");
    }

  } catch (error) {
    if (error instanceof Error && error.message.startsWith("Runner preflight")) {
      throw error;
    }
    throw new Error("Runner preflight request failed");
  } finally {
    clearTimeout(timeout);
  }
}
