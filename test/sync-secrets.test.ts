import { describe, expect, it, vi } from "vitest";
import { assertProductionSecretSyncProfile, validateBasicCredentialSecrets } from "../tools/sync-secrets.ts";

const credentials = {
  close: {
    source: { env: "CLOSE_API_KEY", header: "Authorization", value_prefix: "Basic " },
    upstream: { header: "Authorization", secret: "CLOSE_API_KEY", value_prefix: "Basic " },
  },
};

describe("secret synchronization preflight", () => {
  it("allows remote secret writes only from the production profile", () => {
    expect(() => assertProductionSecretSyncProfile("prod")).not.toThrow();
    expect(() => assertProductionSecretSyncProfile("local")).toThrow(/requires the active prod environment profile/);
    expect(() => assertProductionSecretSyncProfile(null)).toThrow(/requires the active prod environment profile/);
  });




  it("accepts complete raw Basic credential pairs", () => {
    expect(() => validateBasicCredentialSecrets(credentials, {
      CLOSE_API_KEY: "api-key:",
    })).not.toThrow();
    expect(() => validateBasicCredentialSecrets(credentials, {
      CLOSE_API_KEY: "service-user:pässword:with-colon",
    })).not.toThrow();
  });

  it("rejects malformed raw Basic credentials", () => {
    expect(() => validateBasicCredentialSecrets(credentials, {
      CLOSE_API_KEY: "missing-delimiter",
    })).toThrow(/username:password/);
    expect(() => validateBasicCredentialSecrets(credentials, {
      CLOSE_API_KEY: "user:password\u0000suffix",
    })).toThrow(/username:password/);
  });
});
