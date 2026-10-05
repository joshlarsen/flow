import { describe, expect, it } from "vitest";
import { runtimeEnvironment } from "../src/runtime-environment.ts";

describe("container runtime environment", () => {
  it("forwards exactly the declared plaintext bindings", () => {
    expect(runtimeEnvironment(
      { SERVICE_URL: "https://service.example", UNDECLARED: "hidden" },
      ["SERVICE_URL"],
    )).toEqual({ SERVICE_URL: "https://service.example" });
  });

  it("rejects missing or blank declared bindings", () => {
    expect(() => runtimeEnvironment({}, ["SERVICE_URL"])).toThrow(/SERVICE_URL/);
    expect(() => runtimeEnvironment({ SERVICE_URL: "   " }, ["SERVICE_URL"])).toThrow(/SERVICE_URL/);
  });
});
