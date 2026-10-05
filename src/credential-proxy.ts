import { appConfig } from "./generated-config.ts";
import { apiError } from "./http.ts";
import type { OAuthCredentialBroker } from "./oauth-broker.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });
const basicAuthValuePrefix = "Basic ";
const basicAuthorizationPattern = /^Basic +([A-Za-z0-9+/]+={0,2})$/i;

interface BasicCredentialParseResult {
  readonly credential: readonly [string, string] | null;
  readonly decoded: string | null;
  readonly error: "syntax" | "base64" | "noncanonical_base64" | "utf8" | "control_character" | "missing_separator" | null;
}

export interface CredentialInjectionRule {
  readonly targetKind?: "provider" | "route";
  readonly targetName?: string;
  readonly urlPrefix: string;
  readonly credentialName: string;
  readonly sourceHeader: string;
  readonly sourceValuePrefix: string;
  readonly upstream:
    | {
        readonly kind: "static";
        readonly header: string;
        readonly secret: string;
        readonly valuePrefix: string;
      }
    | { readonly kind: "oauth" };
}

export interface CredentialTarget {
  readonly kind: "provider" | "route";
  readonly name: string;
}

/** Compares two strings without returning early on the first differing byte. */
function safeEqual(left: string, right: string): boolean {
  let mismatch = left.length ^ right.length;
  for (let index = 0; index < left.length; index++) {
    mismatch |= left.charCodeAt(index) ^ (right.charCodeAt(index) || 0);
  }
  return mismatch === 0;
}

function hasControlCharacter(value: string): boolean {
  return /[\u0000-\u001f\u007f]/u.test(value);
}

/** Decodes a canonical or unpadded UTF-8 Basic credential into its two fields. */
function parseBasicCredential(value: string): BasicCredentialParseResult {
  const match = basicAuthorizationPattern.exec(value);
  if (!match) return { credential: null, decoded: null, error: "syntax" };
  const encoded = match[1]!;
  if (encoded.length % 4 === 1) return { credential: null, decoded: null, error: "base64" };

  let binary: string;
  try {
    binary = atob(encoded);
  } catch {
    return { credential: null, decoded: null, error: "base64" };
  }
  if (!safeEqual(btoa(binary).replace(/=+$/u, ""), encoded.replace(/=+$/u, ""))) {
    return { credential: null, decoded: null, error: "noncanonical_base64" };
  }

  let decoded: string;
  try {
    decoded = decoder.decode(Uint8Array.from(binary, (character) => character.charCodeAt(0)));
  } catch {
    return { credential: null, decoded: null, error: "utf8" };
  }
  if (hasControlCharacter(decoded)) return { credential: null, decoded, error: "control_character" };
  const separator = decoded.indexOf(":");
  return separator < 0
    ? { credential: null, decoded, error: "missing_separator" }
    : { credential: [decoded.slice(0, separator), decoded.slice(separator + 1)], decoded, error: null };
}

/** Formats a valid UTF-8 user:password value as a canonical Basic header. */
function encodeBasicCredential(value: string): string | null {
  if (!value.includes(":") || hasControlCharacter(value)) return null;
  let binary = "";
  for (const byte of encoder.encode(value)) binary += String.fromCharCode(byte);
  return `${basicAuthValuePrefix}${btoa(binary)}`;
}

/** Matches either a literal prefixed token or one complete Basic credential field. */
function matchesScopedCredential(value: string, token: string, valuePrefix: string): boolean {
  if (valuePrefix !== basicAuthValuePrefix) return safeEqual(value, `${valuePrefix}${token}`);
  const { credential, decoded, error } = parseBasicCredential(value);
  if (!credential) {
    return error === "missing_separator" && decoded !== null && safeEqual(decoded, token);
  }
  const usernameMatch = safeEqual(credential[0], token) ? 1 : 0;
  const passwordMatch = safeEqual(credential[1], token) ? 1 : 0;
  return (usernameMatch | passwordMatch) === 1;
}

/** Describes Basic parsing and matching without exposing either credential field. */
function basicCredentialDiagnostics(value: string, token: string): Record<string, boolean | string> {
  const { credential, decoded, error } = parseBasicCredential(value);
  return {
    credential_present: value.length > 0,
    basic_decoded: credential !== null,
    ...(error ? { basic_parse_error: error } : {}),
    decoded_token_match: decoded !== null && safeEqual(decoded, token),
    username_token_match: credential !== null && safeEqual(credential[0], token),
    password_token_match: credential !== null && safeEqual(credential[1], token),
  };
}

/** Records a credential proxy outcome without including credential material. */
function logCredentialProxyResponse(
  url: URL,
  status: number,
  outcome: string,
  credentialHeader?: string,
  details: Record<string, boolean | string> = {},
): void {
  console.log(JSON.stringify({
    level: status >= 400 ? "error" : "info",
    source: "worker",
    event: "credential_proxy_response",
    host: url.hostname,
    path: url.pathname,
    ...(credentialHeader ? { credential_header: credentialHeader } : {}),
    status,
    outcome,
    ...details,
  }));
}

/** Derives the scoped credential accepted only for one container's intercepted egress. */
export async function containerProxyToken(secret: string, containerId: string): Promise<string> {
  const key = await crypto.subtle.importKey("raw", encoder.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const signature = new Uint8Array(await crypto.subtle.sign("HMAC", key, encoder.encode(containerId)));
  return btoa(String.fromCharCode(...signature)).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

/** Recognizes a scoped token in any source format declared by the deployment. */
function isScopedCredentialValue(value: string, token: string, rules: readonly CredentialInjectionRule[]): boolean {
  return safeEqual(value, token) || rules.some((rule) =>
    matchesScopedCredential(value, token, rule.sourceValuePrefix)
  );
}

/** Checks whether any outbound header carries this container's scoped token. */
function hasScopedCredential(request: Request, token: string, rules: readonly CredentialInjectionRule[]): boolean {
  let found = false;
  request.headers.forEach((value) => {
    found ||= isScopedCredentialValue(value, token, rules);
  });
  return found;
}

/** Removes every copy of the scoped token before adding the real upstream credential. */
function stripScopedCredentials(headers: Headers, token: string, rules: readonly CredentialInjectionRule[]): void {
  const scopedHeaders: string[] = [];
  headers.forEach((value, name) => {
    if (isScopedCredentialValue(value, token, rules)) scopedHeaders.push(name);
  });
  for (const name of scopedHeaders) {
    headers.delete(name);
  }
}

function matchingCredentialRule(
  request: Request,
  rules: readonly CredentialInjectionRule[],
): CredentialInjectionRule | undefined {
  const url = new URL(request.url);
  return rules.find((candidate) => {
    const prefix = new URL(candidate.urlPrefix);
    const prefixPath = prefix.pathname.replace(/\/$/, "");
    return url.origin === prefix.origin && (url.pathname === prefixPath || url.pathname.startsWith(`${prefixPath}/`));
  });
}

function upstreamHeader(rule: CredentialInjectionRule | undefined): string | undefined {
  if (!rule) return undefined;
  return rule.upstream.kind === "oauth" ? "authorization" : rule.upstream.header;
}

/** Returns only the non-secret catalog identity needed to classify an egress span. */
export function credentialTarget(
  request: Request,
  rules: readonly CredentialInjectionRule[] = appConfig.credentialInjections,
): CredentialTarget | null {
  const rule = matchingCredentialRule(request, rules);
  return rule?.targetKind && rule.targetName
    ? { kind: rule.targetKind, name: rule.targetName }
    : null;
}

/** Authenticates intercepted egress and replaces its scoped token with a Worker Secret. */
export async function injectCredential(
  request: Request,
  environment: Record<string, unknown>,
  containerId: string,
  rules: readonly CredentialInjectionRule[] = appConfig.credentialInjections,
): Promise<Response> {
  const url = new URL(request.url);
  const rule = matchingCredentialRule(request, rules);
  const runnerSecret = environment[appConfig.authSecretName];
  if (typeof runnerSecret !== "string" || !runnerSecret) {
    logCredentialProxyResponse(url, 500, "missing_runner_secret", upstreamHeader(rule));
    return apiError(500, "missing_runner_secret", "Runner API secret is not configured");
  }
  const expectedToken = await containerProxyToken(runnerSecret, containerId);
  if (!rule) {
    if (hasScopedCredential(request, expectedToken, rules)) {
      logCredentialProxyResponse(url, 403, "credential_path_denied");
      return apiError(403, "credential_path_denied", "No credential rule permits this URL");
    }
    return fetch(request);
  }
  const presentedCredential = request.headers.get(rule.sourceHeader) ?? "";
  if (!matchesScopedCredential(presentedCredential, expectedToken, rule.sourceValuePrefix)) {
    logCredentialProxyResponse(
      url,
      401,
      "invalid_proxy_token",
      rule.sourceHeader,
      rule.sourceValuePrefix === basicAuthValuePrefix
        ? basicCredentialDiagnostics(presentedCredential, expectedToken)
        : { credential_present: presentedCredential.length > 0 },
    );
    return apiError(401, "invalid_proxy_token", "Invalid container proxy credential");
  }

  const headers = new Headers(request.headers);
  stripScopedCredentials(headers, expectedToken, rules);
  headers.delete(rule.sourceHeader);
  headers.delete("authorization");
  headers.delete("host");
  headers.delete("cookie");
  headers.delete("cf-connecting-ip");
  headers.delete("x-forwarded-for");

  let oauthBroker: DurableObjectStub<OAuthCredentialBroker> | null = null;
  let oauthGeneration: string | null = null;
  if (rule.upstream.kind === "oauth") {
    const namespace = (environment as { OAUTH_CREDENTIAL_BROKER?: DurableObjectNamespace<OAuthCredentialBroker> }).OAUTH_CREDENTIAL_BROKER;
    if (!namespace) {
      logCredentialProxyResponse(url, 500, "missing_oauth_broker", "authorization");
      return apiError(500, "missing_oauth_broker", "OAuth credential broker is not configured");
    }
    oauthBroker = namespace.get(namespace.idFromName(rule.credentialName));
    const result = await oauthBroker.authorization(rule.credentialName);
    if (!result.ok) {
      logCredentialProxyResponse(url, result.status, result.code, "authorization");
      return apiError(result.status, result.code, result.message);
    }
    headers.set("authorization", result.authorization);
    oauthGeneration = result.generation;
  } else {
    const credential = environment[rule.upstream.secret];
    if (typeof credential !== "string" || !credential) {
      logCredentialProxyResponse(url, 500, "missing_credential_secret", rule.upstream.header);
      return apiError(500, "missing_credential_secret", `Credential secret ${rule.upstream.secret} is not configured`);
    }
    const upstreamCredential = rule.upstream.valuePrefix === basicAuthValuePrefix
      ? encodeBasicCredential(credential)
      : `${rule.upstream.valuePrefix}${credential}`;
    if (upstreamCredential === null) {
      logCredentialProxyResponse(url, 500, "invalid_credential_secret", rule.upstream.header);
      return apiError(500, "invalid_credential_secret", `Credential secret ${rule.upstream.secret} is not a valid Basic username:password value`);
    }
    headers.set(rule.upstream.header, upstreamCredential);
  }
  const upstream = await fetch(new Request(request, { headers }));
  if (upstream.status === 401 && oauthBroker && oauthGeneration) {
    try {
      await oauthBroker.invalidate(rule.credentialName, oauthGeneration);
    } catch {
      logCredentialProxyResponse(url, 500, "oauth_invalidation_failed", "authorization");
    }
  }
  logCredentialProxyResponse(url, upstream.status, "upstream_response", upstreamHeader(rule));
  return upstream;
}
