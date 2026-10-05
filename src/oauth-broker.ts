import { DurableObject, tracing } from "cloudflare:workers";
import { appConfig } from "./generated-config.ts";
import {
  OAuthTokenManager,
  type OAuthAuthorizationResult,
  type OAuthCredentialConfig,
  type OAuthPolicy,
} from "./oauth.ts";
import { spanNames, traceAsync } from "./tracing.ts";
import type { Env } from "./types.ts";

const oauthCredentials = appConfig.oauthCredentials as Readonly<Record<string, OAuthCredentialConfig>>;
const oauthPolicy = appConfig.oauthPolicy as OAuthPolicy;

/** Coordinates one ephemeral OAuth token cache for each configured credential. */
export class OAuthCredentialBroker extends DurableObject<Env> {
  private credentialName: string | null = null;
  private manager: OAuthTokenManager | null = null;

  /** Returns a bearer value without exposing seed credentials outside the Worker. */
  async authorization(credentialName: string): Promise<OAuthAuthorizationResult> {
    const config = oauthCredentials[credentialName];
    if (!config) {
      return { ok: false, status: 500, code: "unknown_oauth_credential", message: "OAuth credential is not configured" };
    }
    if (this.credentialName !== null && this.credentialName !== credentialName) {
      return { ok: false, status: 500, code: "oauth_broker_mismatch", message: "OAuth credential broker identity does not match the requested credential" };
    }
    this.credentialName = credentialName;
    this.manager ??= new OAuthTokenManager(config, oauthPolicy, this.env as unknown as Record<string, unknown>);

    return traceAsync(tracing, spanNames.oauthToken, {
      "agent_runner.credential.name": credentialName,
      "agent_runner.oauth.grant_type": config.grant.type,
      "server.address": new URL(config.tokenUrl).hostname,
    }, async (span) => {
      const result = await this.manager!.authorization();
      span.setAttribute("agent_runner.oauth.cache", result.ok ? result.cache : "miss");
      span.setAttribute("agent_runner.outcome", result.ok ? "success" : result.status === 503 ? "error" : "rejected");
      if (!result.ok) {
        span.setAttribute("error.type", result.code);
        if (result.upstreamStatus !== undefined) span.setAttribute("http.response.status_code", result.upstreamStatus);
      }
      console.log(JSON.stringify({
        level: result.ok ? "info" : "error",
        source: "worker",
        event: "oauth_token_response",
        credential: credentialName,
        grant_type: config.grant.type,
        token_endpoint_host: new URL(config.tokenUrl).hostname,
        cache: result.ok ? result.cache : "miss",
        outcome: result.ok ? "success" : result.code,
        ...(result.ok || result.upstreamStatus === undefined ? {} : { upstream_status: result.upstreamStatus }),
      }));
      return result;
    });
  }

  /** Invalidates a rejected access token without clearing a newer generation. */
  async invalidate(credentialName: string, generation: string): Promise<void> {
    if (this.credentialName === credentialName) this.manager?.invalidate(generation);
  }
}
