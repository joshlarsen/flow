import { createHash, randomBytes, timingSafeEqual, randomUUID } from "node:crypto";
import { chmod, lstat, open, readFile, rename, unlink } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadConfigurationEnvironments, loadRunnerConfig, type RunnerConfig } from "./config.ts";

export const GOOGLE_AUTHORIZATION_URL = "https://accounts.google.com/o/oauth2/v2/auth";
export const GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token";
export const GOOGLE_REVOKE_URL = "https://oauth2.googleapis.com/revoke";
export const GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive";
export const GOOGLE_GMAIL_READONLY_SCOPE = "https://www.googleapis.com/auth/gmail.readonly";
export const GOOGLE_WORKSPACE_SCOPES = [GOOGLE_DRIVE_SCOPE, GOOGLE_GMAIL_READONLY_SCOPE] as const;
const GOOGLE_DRIVE_ABOUT_URL = "https://www.googleapis.com/drive/v3/about?fields=user(emailAddress)";
const MAX_RESPONSE_BYTES = 65_536;
const MAX_TOKEN_BYTES = 16_384;
const CALLBACK_TIMEOUT_MS = 5 * 60_000;
const NETWORK_TIMEOUT_MS = 30_000;

export interface GoogleTokens {
  accessToken: string;
  refreshToken: string;
  expiresIn: number;
}

export interface LoopbackCallback {
  redirectUri: string;
  waitForCode: Promise<string>;
  close(): Promise<void>;
}

type Fetch = typeof fetch;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function requiredString(value: unknown, field: string, maxBytes = MAX_TOKEN_BYTES): string {
  if (typeof value !== "string" || !value || /[\u0000-\u001f\u007f\s]/u.test(value)) {
    throw new Error(`Google OAuth response contains an invalid ${field}`);
  }
  if (Buffer.byteLength(value) > maxBytes) throw new Error(`Google OAuth response ${field} is too large`);
  return value;
}

/** Reads a response body without allowing an OAuth endpoint to exhaust local memory. */
async function readBoundedText(response: Response, maximumBytes = MAX_RESPONSE_BYTES): Promise<string> {
  const declaredLength = Number(response.headers.get("content-length"));
  if (Number.isFinite(declaredLength) && declaredLength > maximumBytes) {
    throw new Error("Google OAuth response is too large");
  }
  if (!response.body) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > maximumBytes) {
      await reader.cancel();
      throw new Error("Google OAuth response is too large");
    }
    chunks.push(value);
  }
  return Buffer.concat(chunks).toString("utf8");
}

async function readBoundedJson(response: Response): Promise<unknown> {
  const body = await readBoundedText(response);
  try {
    return JSON.parse(body) as unknown;
  } catch {
    throw new Error("Google returned a malformed JSON response");
  }
}

function formRequest(body: URLSearchParams): RequestInit {
  return {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/x-www-form-urlencoded" },
    body,
    redirect: "error",
    signal: AbortSignal.timeout(NETWORK_TIMEOUT_MS),
  };
}

/** Constructs Google's installed-app consent request with offline access and PKCE. */
export function buildGoogleAuthorizationURL(options: {
  clientId: string;
  redirectUri: string;
  account: string;
  state: string;
  codeChallenge: string;
}): URL {
  const url = new URL(GOOGLE_AUTHORIZATION_URL);
  url.search = new URLSearchParams({
    client_id: options.clientId,
    redirect_uri: options.redirectUri,
    response_type: "code",
    scope: GOOGLE_WORKSPACE_SCOPES.join(" "),
    access_type: "offline",
    prompt: "consent select_account",
    include_granted_scopes: "true",
    login_hint: options.account,
    state: options.state,
    code_challenge: options.codeChallenge,
    code_challenge_method: "S256",
  }).toString();
  return url;
}

/** Exchanges an authorization code and validates the bounded Google token response. */
export async function exchangeGoogleAuthorizationCode(options: {
  clientId: string;
  clientSecret: string;
  code: string;
  codeVerifier: string;
  redirectUri: string;
}, fetchImplementation: Fetch = fetch): Promise<GoogleTokens> {
  const response = await fetchImplementation(GOOGLE_TOKEN_URL, formRequest(new URLSearchParams({
    code: options.code,
    client_id: options.clientId,
    client_secret: options.clientSecret,
    redirect_uri: options.redirectUri,
    grant_type: "authorization_code",
    code_verifier: options.codeVerifier,
  })));
  const value = await readBoundedJson(response);
  if (!response.ok) throw new Error(`Google OAuth code exchange failed with HTTP ${response.status}`);
  if (!isRecord(value)) throw new Error("Google OAuth response must be a JSON object");
  if (typeof value.token_type !== "string" || value.token_type.toLowerCase() !== "bearer") {
    throw new Error("Google OAuth response did not return a Bearer token");
  }
  if (typeof value.expires_in !== "number" || !Number.isFinite(value.expires_in) || value.expires_in <= 0) {
    throw new Error("Google OAuth response contains an invalid expires_in");
  }
  const grantedScopes = new Set(typeof value.scope === "string" ? value.scope.split(/\s+/u) : []);
  const missingScope = GOOGLE_WORKSPACE_SCOPES.find((scope) => !grantedScopes.has(scope));
  if (missingScope) {
    throw new Error(`Google did not grant the required scope ${missingScope}`);
  }
  return {
    accessToken: requiredString(value.access_token, "access_token"),
    refreshToken: requiredString(value.refresh_token, "refresh_token"),
    expiresIn: value.expires_in,
  };
}

/** Resolves the Google Drive identity and requires the dedicated account exactly. */
export async function verifyGoogleWorkspaceAccount(
  accessToken: string,
  expectedAccount: string,
  fetchImplementation: Fetch = fetch,
): Promise<string> {
  const response = await fetchImplementation(GOOGLE_DRIVE_ABOUT_URL, {
    headers: { accept: "application/json", authorization: `Bearer ${accessToken}` },
    redirect: "error",
    signal: AbortSignal.timeout(NETWORK_TIMEOUT_MS),
  });
  const value = await readBoundedJson(response);
  if (!response.ok) throw new Error(`Google Drive account verification failed with HTTP ${response.status}`);
  const email = isRecord(value) && isRecord(value.user) && typeof value.user.emailAddress === "string"
    ? value.user.emailAddress.trim()
    : "";
  if (!email || email.toLowerCase() !== expectedAccount.toLowerCase()) {
    throw new Error(`Authorized Google account does not match the expected account ${expectedAccount}`);
  }
  return email;
}

/** Revokes a newly issued token after verification failure without surfacing cleanup details. */
export async function revokeGoogleToken(token: string, fetchImplementation: Fetch = fetch): Promise<void> {
  try {
    await fetchImplementation(GOOGLE_REVOKE_URL, formRequest(new URLSearchParams({ token })));
  } catch {
    // Revocation is best effort; the original verification error is more useful and secret-safe.
  }
}

/** Exchanges a code and revokes the issued refresh token if account verification fails. */
export async function exchangeAndVerifyGoogleAuthorization(options: {
  clientId: string;
  clientSecret: string;
  code: string;
  codeVerifier: string;
  redirectUri: string;
  expectedAccount: string;
}, fetchImplementation: Fetch = fetch): Promise<{ tokens: GoogleTokens; verifiedAccount: string }> {
  const tokens = await exchangeGoogleAuthorizationCode(options, fetchImplementation);
  try {
    const verifiedAccount = await verifyGoogleWorkspaceAccount(tokens.accessToken, options.expectedAccount, fetchImplementation);
    return { tokens, verifiedAccount };
  } catch (error) {
    await revokeGoogleToken(tokens.refreshToken, fetchImplementation);
    throw error;
  }
}

function stateMatches(actual: string, expected: string): boolean {
  const actualBytes = Buffer.from(actual);
  const expectedBytes = Buffer.from(expected);
  return actualBytes.length === expectedBytes.length && timingSafeEqual(actualBytes, expectedBytes);
}

function callbackPage(message: string): string {
  return `<!doctype html><meta charset="utf-8"><meta name="robots" content="noindex"><title>Google authorization</title><p>${message}</p>`;
}

/** Starts a one-use callback server bound only to a random IPv4 loopback port. */
export async function startLoopbackCallback(expectedState: string, timeoutMs = CALLBACK_TIMEOUT_MS): Promise<LoopbackCallback> {
  let resolveCode!: (code: string) => void;
  let rejectCode!: (error: Error) => void;
  let settled = false;
  let timer: NodeJS.Timeout;
  const waitForCode = new Promise<string>((resolve, reject) => {
    resolveCode = resolve;
    rejectCode = reject;
  });
  const settle = (error?: Error, code?: string) => {
    if (settled) return;
    settled = true;
    clearTimeout(timer);
    if (error) rejectCode(error); else resolveCode(code!);
  };
  const server: Server = createServer((request, response) => {
    response.setHeader("cache-control", "no-store");
    response.setHeader("connection", "close");
    response.setHeader("content-type", "text/html; charset=utf-8");
    if (!request.url || request.url.length > 8192) {
      response.writeHead(400).end(callbackPage("Authorization callback was invalid. You may close this tab."));
      settle(new Error("Google OAuth callback was invalid"));
      return;
    }
    const url = new URL(request.url, "http://127.0.0.1");
    if (request.method !== "GET" || url.pathname !== "/") {
      response.writeHead(404).end(callbackPage("Not found."));
      return;
    }
    const state = url.searchParams.get("state") ?? "";
    if (!stateMatches(state, expectedState)) {
      response.writeHead(400).end(callbackPage("Authorization state did not match. You may close this tab."));
      settle(new Error("Google OAuth callback state did not match"));
      return;
    }
    if (url.searchParams.has("error")) {
      response.writeHead(400).end(callbackPage("Authorization was not granted. You may close this tab."));
      settle(new Error("Google OAuth authorization was not granted"));
      return;
    }
    const code = url.searchParams.get("code") ?? "";
    if (!code || Buffer.byteLength(code) > MAX_TOKEN_BYTES || /[\u0000-\u001f\u007f\s]/u.test(code)) {
      response.writeHead(400).end(callbackPage("Authorization code was invalid. You may close this tab."));
      settle(new Error("Google OAuth callback did not contain a valid authorization code"));
      return;
    }
    response.writeHead(200).end(callbackPage("Authorization succeeded. You may close this tab and return to the terminal."));
    settle(undefined, code);
  });
  await new Promise<void>((resolve, reject) => {
    const handleError = (error: Error) => reject(error);
    server.once("error", handleError);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", handleError);
      resolve();
    });
  });
  server.on("error", (error) => settle(error));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Could not allocate a Google OAuth loopback port");
  timer = setTimeout(() => settle(new Error("Timed out waiting for the Google OAuth callback")), timeoutMs);
  return {
    redirectUri: `http://127.0.0.1:${address.port}`,
    waitForCode,
    close: async () => {
      clearTimeout(timer);
      if (!settled) settle(new Error("Google OAuth callback server closed"));
      if (!server.listening) return;
      await new Promise<void>((resolve, reject) => {
        server.close((error) => error ? reject(error) : resolve());
        server.closeAllConnections();
      });
    },
  };
}

function secretAssignmentPattern(name: string): RegExp {
  return new RegExp(`^(\\s*(?:export\\s+)?${name}\\s*=).*$`);
}

/** Atomically replaces one dotenv assignment while preserving all unrelated content. */
export async function upsertSecretFile(file: string, name: string, value: string): Promise<void> {
  if (!/^[A-Z][A-Z0-9_]*$/.test(name)) throw new Error("Invalid Worker Secret name");
  if (!/^[A-Za-z0-9._~+\/-]+$/.test(value) || Buffer.byteLength(value) > MAX_TOKEN_BYTES) {
    throw new Error("Google refresh token cannot be represented safely in the active secrets profile");
  }
  let contents = "";
  try {
    const metadata = await lstat(file);
    if (metadata.isSymbolicLink() || !metadata.isFile()) throw new Error(`${file} must be a regular file, not a symlink`);
    if (metadata.size > 1024 * 1024) throw new Error(`${file} is too large to update safely`);
    contents = await readFile(file, "utf8");
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) throw error;
  }
  const pattern = secretAssignmentPattern(name);
  const lines = contents.split("\n");
  const matchingLines = lines.flatMap((line, index) => pattern.test(line) ? [index] : []);
  if (matchingLines.length > 1) throw new Error(`${name} is assigned more than once in ${file}`);
  if (matchingLines.length === 1) {
    const index = matchingLines[0]!;
    lines[index] = lines[index]!.replace(pattern, `$1${value}`);
    contents = lines.join("\n");
  } else {
    const separator = contents.length === 0 || contents.endsWith("\n") ? "" : "\n";
    contents = `${contents}${separator}${name}=${value}\n`;
  }
  const temporaryFile = path.join(path.dirname(file), `.${path.basename(file)}.${randomUUID()}.tmp`);
  let handle;
  try {
    handle = await open(temporaryFile, "wx", 0o600);
    await handle.writeFile(contents, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporaryFile, file);
    await chmod(file, 0o600);
  } catch (error) {
    await handle?.close().catch(() => undefined);
    await unlink(temporaryFile).catch(() => undefined);
    throw error;
  }
}

function resolveGoogleCredential(config: RunnerConfig, credentialName: string) {
  const credential = config.credentials[credentialName];
  if (!credential) throw new Error(`Unknown credential ${credentialName}`);
  if (!("oauth" in credential.upstream)) throw new Error(`${credentialName} is not an OAuth credential`);
  const oauth = credential.upstream.oauth;
  if (oauth.token_url !== GOOGLE_TOKEN_URL) throw new Error(`${credentialName} must use ${GOOGLE_TOKEN_URL}`);
  if (oauth.grant.type !== "refresh_token") throw new Error(`${credentialName} must use the refresh_token grant`);
  if (oauth.client_auth.method !== "client_secret_post") {
    throw new Error(`${credentialName} must use client_secret_post for a Google Desktop OAuth client`);
  }
  return { oauth, refreshTokenSecret: oauth.grant.refresh_token_secret, clientSecret: oauth.client_auth.secret };
}

function requiredLocalSecret(values: Record<string, string | undefined>, name: string): string {
  const value = values[name];
  if (!value?.trim()) throw new Error(`Missing ${name} in the active secrets profile or the process environment`);
  if (Buffer.byteLength(value) > MAX_TOKEN_BYTES || /[\u0000-\u001f\u007f]/u.test(value)) {
    throw new Error(`${name} contains an invalid local secret value`);
  }
  return value;
}

function validateAccount(account: string): string {
  const normalized = account.trim();
  if (normalized.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(normalized)) {
    throw new Error("--account must be the dedicated Google Workspace account email address");
  }
  return normalized;
}

/** Runs installed-app consent, verifies identity, and persists only the refresh token. */
export async function bootstrapGoogleOAuth(options: {
  root: string;
  credential: string;
  account: string;
  fetchImplementation?: Fetch;
  output?: (message: string) => void;
}): Promise<void> {
  const account = validateAccount(options.account);
  const config = await loadRunnerConfig(options.root);
  const definition = resolveGoogleCredential(config, options.credential);
  const environments = await loadConfigurationEnvironments(options.root);
  const clientId = requiredLocalSecret(environments.secrets, definition.oauth.client_id_secret);
  const clientSecret = requiredLocalSecret(environments.secrets, definition.clientSecret);
  const fetchImplementation = options.fetchImplementation ?? fetch;
  const output = options.output ?? console.log;
  const state = Buffer.from(randomBytes(32)).toString("base64url");
  const codeVerifier = Buffer.from(randomBytes(64)).toString("base64url");
  const codeChallenge = createHash("sha256").update(codeVerifier).digest("base64url");
  const callback = await startLoopbackCallback(state);
  try {
    const authorizationUrl = buildGoogleAuthorizationURL({ clientId, redirectUri: callback.redirectUri, account, state, codeChallenge });
    output(`Open this URL in a browser and authorize ${account}:\n${authorizationUrl.toString()}`);
    const code = await callback.waitForCode;
    const { tokens, verifiedAccount } = await exchangeAndVerifyGoogleAuthorization({
      clientId,
      clientSecret,
      code,
      codeVerifier,
      redirectUri: callback.redirectUri,
      expectedAccount: account,
    }, fetchImplementation);
    try {
      await upsertSecretFile(environments.files.secrets, definition.refreshTokenSecret, tokens.refreshToken);
    } catch (error) {
      await revokeGoogleToken(tokens.refreshToken, fetchImplementation);
      throw error;
    }
    output(`Stored ${definition.refreshTokenSecret} for verified Google account ${verifiedAccount}.`);
    output("Run pnpm secrets:sync to deploy the updated Worker Secret.");
  } finally {
    await callback.close();
  }
}

function parseOptions(argv: string[]) {
  const args = argv[0] === "--" ? argv.slice(1) : argv;
  const { values } = parseArgs({
    args,
    options: {
      credential: { type: "string" },
      account: { type: "string" },
    },
    strict: true,
    allowPositionals: false,
  });
  if (!values.credential) throw new Error("Missing required --credential <name>");
  if (!values.account) throw new Error("Missing required --account <email>");
  return { credential: values.credential, account: values.account };
}

async function main(): Promise<void> {
  const options = parseOptions(process.argv.slice(2));
  await bootstrapGoogleOAuth({ root: process.cwd(), ...options });
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    process.stderr.write(`${error instanceof Error ? error.message : "Google OAuth bootstrap failed"}\n`);
    process.exitCode = 1;
  });
}
