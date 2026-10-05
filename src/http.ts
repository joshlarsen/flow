import { appConfig } from "./generated-config.ts";

const encoder = new TextEncoder();

/** Builds a non-cacheable JSON response with the supplied status and headers. */
export function json(
  data: unknown,
  status = 200,
  headers?: HeadersInit,
): Response {
  const responseHeaders = new Headers(headers);
  responseHeaders.set("content-type", "application/json; charset=utf-8");
  responseHeaders.set("cache-control", "no-store");
  return new Response(JSON.stringify(data), {
    status,
    headers: responseHeaders,
  });
}

/** Builds the API's standard structured error response. */
export function apiError(
  status: number,
  code: string,
  message: string,
  extra: Record<string, unknown> = {},
  headers?: HeadersInit,
): Response {
  return json({ error: { code, message }, ...extra }, status, headers);
}

/** Returns the lowercase hexadecimal SHA-256 digest of a string. */
export async function sha256(input: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(input));
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

/** Validates the request bearer token without directly comparing secret text. */
export async function isAuthorized(
  request: Request,
  env: Record<string, unknown>,
): Promise<boolean> {
  const expected = env[appConfig.authSecretName];
  if (typeof expected !== "string" || expected.length < 32) return false;
  const authorization = request.headers.get("authorization");
  if (!authorization?.startsWith("Bearer ")) return false;
  const provided = authorization.slice("Bearer ".length);
  const [expectedDigest, providedDigest] = await Promise.all([
    sha256(expected),
    sha256(provided),
  ]);
  let mismatch = expectedDigest.length ^ providedDigest.length;
  for (let i = 0; i < expectedDigest.length; i++) {
    mismatch |=
      expectedDigest.charCodeAt(i) ^ (providedDigest.charCodeAt(i) || 0);
  }
  return mismatch === 0;
}

export type CreateJobBody = Record<string, never>;

/** Reads and strictly validates a bounded job-creation body. */
export async function readCreateJobBody(
  request: Request,
): Promise<CreateJobBody | Response> {
  if (!request.body) return {};
  const maxRequestBytes = 1024;
  const declaredLength = Number(request.headers.get("content-length") ?? 0);
  if (declaredLength > maxRequestBytes) {
    return apiError(
      413,
      "request_too_large",
      "Request body must be at most 1024 bytes",
    );
  }

  let value: unknown;
  try {
    const reader = request.body.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (true) {
      const { done, value: chunk } = await reader.read();
      if (done) break;
      total += chunk.byteLength;
      if (total > maxRequestBytes) {
        await reader.cancel();
        return apiError(
          413,
          "request_too_large",
          "Request body must be at most 1024 bytes",
        );
      }
      chunks.push(chunk);
    }
    if (total === 0) return {};
    if (
      !request.headers
        .get("content-type")
        ?.toLowerCase()
        .startsWith("application/json")
    ) {
      return apiError(
        415,
        "unsupported_media_type",
        "Content-Type must be application/json",
      );
    }
    const body = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      body.set(chunk, offset);
      offset += chunk.byteLength;
    }
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body));
  } catch {
    return apiError(400, "invalid_json", "Request body must be valid JSON");
  }

  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return apiError(
      400,
      "invalid_request",
      "Request body must be an empty object",
    );
  }
  if (Object.keys(value).length !== 0)
    return apiError(
      400,
      "invalid_request",
      "Request body must be an empty object",
    );
  return {};
}

export function jobPath(jobId: string): string {
  return `/v1/jobs/${encodeURIComponent(jobId)}`;
}

export function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    value,
  );
}

/** Reads callback bodies with a streaming limit before allocating a full payload. */
export async function readCallbackBody(
  request: Request,
  maximum: number,
): Promise<string | Response> {
  if (!request.body) return "";
  const reader = request.body.getReader(),
    chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > maximum) {
        await reader.cancel();
        return apiError(413, "payload_too_large", "Callback body is too large");
      }
      chunks.push(value);
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.length;
    }
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return apiError(
      400,
      "invalid_request",
      "Callback body must be valid UTF-8",
    );
  }
}
