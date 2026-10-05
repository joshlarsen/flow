const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const bearerTokenPattern = /^[A-Za-z0-9\-._~+/]+=*$/;

export interface OAuthCredentialConfig {
  readonly tokenUrl: string;
  readonly clientIdSecret: string;
  readonly clientAuth:
    | { readonly method: "client_secret_basic" | "client_secret_post"; readonly secret: string }
    | { readonly method: "none" };
  readonly grant:
    | { readonly type: "client_credentials" }
    | { readonly type: "refresh_token"; readonly refreshTokenSecret: string };
  readonly scopes: readonly string[];
  readonly extraParameters: Readonly<Record<string, string>>;
  readonly fallbackTtlMs: number | null;
}

export interface OAuthPolicy {
  readonly tokenRequestTimeoutMs: number;
  readonly expirySkewMs: number;
  readonly maxTokenRequestBytes: number;
  readonly maxTokenResponseBytes: number;
  readonly maxTokenBytes: number;
}

export interface OAuthAuthorization {
  readonly ok: true;
  readonly authorization: string;
  readonly generation: string;
  readonly cache: "hit" | "miss";
}

export interface OAuthAuthorizationError {
  readonly ok: false;
  readonly status: 500 | 502 | 503;
  readonly code: string;
  readonly message: string;
  readonly upstreamStatus?: number;
}

export type OAuthAuthorizationResult = OAuthAuthorization | OAuthAuthorizationError;

interface ResolvedOAuthInputs {
  readonly clientId: string;
  readonly clientSecret?: string;
  readonly refreshToken?: string;
  readonly fingerprint: string;
}

interface CachedAuthorization {
  readonly authorization: string;
  readonly generation: string;
  readonly usableUntil: number;
  readonly fingerprint: string;
}

function error(
  status: OAuthAuthorizationError["status"],
  code: string,
  message: string,
  upstreamStatus?: number,
): OAuthAuthorizationError {
  return { ok: false, status, code, message, ...(upstreamStatus === undefined ? {} : { upstreamStatus }) };
}

function configuredSecret(environment: Record<string, unknown>, name: string): string | null {
  const value = environment[name];
  return typeof value === "string" && value.length > 0 ? value : null;
}

/** Returns bounded exception metadata after removing every credential value in scope. */
function oauthExceptionDiagnostics(value: unknown, secrets: readonly (string | undefined)[]): Record<string, string> {
  const exception = value instanceof Error ? value : new Error("Unknown OAuth request failure");
  const redact = (input: string) => secrets.reduce<string>(
    (result, secret) => secret ? result.replaceAll(secret, "[REDACTED]") : result,
    input,
  ).replace(/[\u0000-\u001f\u007f]/gu, " ").slice(0, 512);
  const cause = exception.cause && typeof exception.cause === "object" ? exception.cause as Record<string, unknown> : null;
  return {
    error_name: redact(exception.name),
    error_message: redact(exception.message),
    ...(typeof cause?.code === "string" ? { cause_code: redact(cause.code) } : {}),
    ...(typeof cause?.name === "string" ? { cause_name: redact(cause.name) } : {}),
    ...(typeof cause?.message === "string" ? { cause_message: redact(cause.message) } : {}),
  };
}

async function sha256(value: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function base64Utf8(value: string): string {
  let binary = "";
  for (const byte of encoder.encode(value)) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function formComponent(value: string): string {
  return new URLSearchParams([["value", value]]).toString().slice("value=".length);
}

/** Reads a token response without trusting its declared or streamed size. */
async function readLimitedResponse(response: Response, limit: number): Promise<Uint8Array | null> {
  const declared = Number(response.headers.get("content-length") ?? 0);
  if (Number.isFinite(declared) && declared > limit) return null;
  if (!response.body) return new Uint8Array();
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.byteLength;
    if (total > limit) {
      await reader.cancel();
      return null;
    }
    chunks.push(value);
  }
  const body = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    body.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return body;
}

function tokenLifetime(value: unknown, fallback: number | null): number | null {
  if (value === undefined) return fallback;
  const seconds = typeof value === "number"
    ? value
    : typeof value === "string" && /^\d+$/u.test(value) ? Number(value) : Number.NaN;
  const milliseconds = seconds * 1000;
  return Number.isSafeInteger(milliseconds) && milliseconds > 0 ? milliseconds : null;
}

/** Holds one activation's OAuth token and serializes token endpoint requests. */
export class OAuthTokenManager {
  private cached: CachedAuthorization | null = null;
  private activeFingerprint: string | null = null;
  private inFlight: Promise<OAuthAuthorizationResult> | null = null;
  private inFlightFingerprint: string | null = null;

  constructor(
    private readonly config: OAuthCredentialConfig,
    private readonly policy: OAuthPolicy,
    private readonly environment: Record<string, unknown>,
    private readonly request: typeof fetch = fetch,
    private readonly now: () => number = Date.now,
  ) {}

  /** Returns a cached bearer value or performs one shared token exchange. */
  async authorization(): Promise<OAuthAuthorizationResult> {
    const inputs = await this.resolveInputs();
    if (!("fingerprint" in inputs)) {
      this.cached = null;
      this.activeFingerprint = null;
      return inputs;
    }
    this.activeFingerprint = inputs.fingerprint;
    const now = this.now();
    if (this.cached?.fingerprint === inputs.fingerprint && now < this.cached.usableUntil) {
      return { ok: true, authorization: this.cached.authorization, generation: this.cached.generation, cache: "hit" };
    }
    if (this.cached?.fingerprint !== inputs.fingerprint) this.cached = null;
    if (this.inFlight && this.inFlightFingerprint === inputs.fingerprint) return this.inFlight;

    const acquisition = this.acquire(inputs);
    this.inFlight = acquisition;
    this.inFlightFingerprint = inputs.fingerprint;
    try {
      return await acquisition;
    } finally {
      if (this.inFlight === acquisition) {
        this.inFlight = null;
        this.inFlightFingerprint = null;
      }
    }
  }

  /** Invalidates only the token generation that produced an upstream 401. */
  invalidate(generation: string): void {
    if (this.cached?.generation === generation) this.cached = null;
  }

  private async resolveInputs(): Promise<ResolvedOAuthInputs | OAuthAuthorizationError> {
    const clientId = configuredSecret(this.environment, this.config.clientIdSecret);
    if (!clientId) return error(500, "missing_credential_secret", `Credential secret ${this.config.clientIdSecret} is not configured`);
    if (encoder.encode(clientId).byteLength > this.policy.maxTokenBytes) {
      return error(500, "invalid_credential_secret", `Credential secret ${this.config.clientIdSecret} exceeds the OAuth token field limit`);
    }
    let clientSecret: string | undefined;
    if (this.config.clientAuth.method !== "none") {
      clientSecret = configuredSecret(this.environment, this.config.clientAuth.secret) ?? undefined;
      if (!clientSecret) return error(500, "missing_credential_secret", `Credential secret ${this.config.clientAuth.secret} is not configured`);
      if (encoder.encode(clientSecret).byteLength > this.policy.maxTokenBytes) {
        return error(500, "invalid_credential_secret", `Credential secret ${this.config.clientAuth.secret} exceeds the OAuth token field limit`);
      }
    }
    let refreshToken: string | undefined;
    if (this.config.grant.type === "refresh_token") {
      refreshToken = configuredSecret(this.environment, this.config.grant.refreshTokenSecret) ?? undefined;
      if (!refreshToken) return error(500, "missing_credential_secret", `Credential secret ${this.config.grant.refreshTokenSecret} is not configured`);
      if (encoder.encode(refreshToken).byteLength > this.policy.maxTokenBytes) {
        return error(500, "invalid_credential_secret", `Credential secret ${this.config.grant.refreshTokenSecret} exceeds the OAuth token field limit`);
      }
    }
    const fingerprint = await sha256(JSON.stringify([this.config, clientId, clientSecret, refreshToken]));
    return { clientId, clientSecret, refreshToken, fingerprint };
  }

  private async acquire(inputs: ResolvedOAuthInputs): Promise<OAuthAuthorizationResult> {
    const parameters = new URLSearchParams(this.config.extraParameters);
    parameters.set("grant_type", this.config.grant.type);
    if (this.config.scopes.length > 0) parameters.set("scope", this.config.scopes.join(" "));
    if (this.config.grant.type === "refresh_token") parameters.set("refresh_token", inputs.refreshToken!);

    const headers = new Headers({
      accept: "application/json",
      "content-type": "application/x-www-form-urlencoded",
    });
    if (this.config.clientAuth.method === "client_secret_basic") {
      headers.set("authorization", `Basic ${base64Utf8(`${formComponent(inputs.clientId)}:${formComponent(inputs.clientSecret!)}`)}`);
    } else {
      parameters.set("client_id", inputs.clientId);
      if (this.config.clientAuth.method === "client_secret_post") parameters.set("client_secret", inputs.clientSecret!);
    }

    const requestBody = parameters.toString();
    const authorizationBytes = encoder.encode(headers.get("authorization") ?? "").byteLength;
    if (encoder.encode(requestBody).byteLength + authorizationBytes > this.policy.maxTokenRequestBytes) {
      return error(500, "oauth_token_request_too_large", "OAuth token request exceeds its configured size limit");
    }

    let response: Response;
    try {
      const request = this.request;
      response = await request(this.config.tokenUrl, {
        method: "POST",
        headers,
        body: requestBody,
        // Cloudflare Workers does not implement redirect="error". Manual mode
        // preserves the fail-closed behavior because every 3xx is rejected below.
        redirect: "manual",
        signal: AbortSignal.timeout(this.policy.tokenRequestTimeoutMs),
      });
    } catch (requestError) {
      console.error(JSON.stringify({
        level: "error",
        source: "worker",
        event: "oauth_token_fetch_failed",
        token_endpoint_host: new URL(this.config.tokenUrl).hostname,
        ...oauthExceptionDiagnostics(requestError, [inputs.clientId, inputs.clientSecret, inputs.refreshToken]),
      }));
      return error(503, "oauth_token_unavailable", "OAuth token endpoint is unavailable");
    }
    if (!response.ok) {
      return response.status === 429 || response.status >= 500
        ? error(503, "oauth_token_unavailable", "OAuth token endpoint is unavailable", response.status)
        : error(502, "oauth_token_rejected", "OAuth token endpoint rejected the configured grant", response.status);
    }

    let body: Uint8Array | null;
    try {
      body = await readLimitedResponse(response, this.policy.maxTokenResponseBytes);
    } catch (responseError) {
      console.error(JSON.stringify({
        level: "error",
        source: "worker",
        event: "oauth_token_response_read_failed",
        token_endpoint_host: new URL(this.config.tokenUrl).hostname,
        ...oauthExceptionDiagnostics(responseError, [inputs.clientId, inputs.clientSecret, inputs.refreshToken]),
      }));
      return error(503, "oauth_token_unavailable", "OAuth token endpoint is unavailable", response.status);
    }
    if (!body) return error(502, "oauth_invalid_token_response", "OAuth token response exceeded its size limit", response.status);
    let value: unknown;
    try {
      value = JSON.parse(decoder.decode(body));
    } catch {
      return error(502, "oauth_invalid_token_response", "OAuth token endpoint returned invalid JSON", response.status);
    }
    if (!value || typeof value !== "object" || Array.isArray(value)) {
      return error(502, "oauth_invalid_token_response", "OAuth token endpoint returned an invalid response", response.status);
    }
    const payload = value as Record<string, unknown>;
    const accessToken = payload.access_token;
    if (
      typeof accessToken !== "string" ||
      accessToken.length === 0 ||
      encoder.encode(accessToken).byteLength > this.policy.maxTokenBytes ||
      !bearerTokenPattern.test(accessToken)
    ) {
      return error(502, "oauth_invalid_token_response", "OAuth token endpoint returned an invalid access token", response.status);
    }
    if (typeof payload.token_type !== "string" || payload.token_type.toLowerCase() !== "bearer") {
      return error(502, "oauth_unsupported_token_type", "OAuth token endpoint did not return a Bearer token", response.status);
    }
    const lifetimeMs = tokenLifetime(payload.expires_in, this.config.fallbackTtlMs);
    if (lifetimeMs === null) {
      return error(502, "oauth_invalid_token_response", "OAuth token response requires a valid expires_in or configured fallback_ttl", response.status);
    }
    if (this.config.grant.type === "refresh_token" && payload.refresh_token !== undefined) {
      const refreshToken = payload.refresh_token;
      if (
        typeof refreshToken !== "string" ||
        encoder.encode(refreshToken).byteLength > this.policy.maxTokenBytes ||
        refreshToken !== inputs.refreshToken
      ) {
        return error(502, "oauth_refresh_token_rotation_unsupported", "OAuth provider rotated the refresh token, which memory-only caching cannot retain", response.status);
      }
    }

    const issuedAt = this.now();
    const skew = Math.min(this.policy.expirySkewMs, lifetimeMs * 0.1);
    const cached: CachedAuthorization = {
      authorization: `Bearer ${accessToken}`,
      generation: crypto.randomUUID(),
      usableUntil: issuedAt + lifetimeMs - skew,
      fingerprint: inputs.fingerprint,
    };
    if (this.activeFingerprint === inputs.fingerprint) this.cached = cached;
    return { ok: true, authorization: cached.authorization, generation: cached.generation, cache: "miss" };
  }
}
