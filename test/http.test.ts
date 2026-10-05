import { describe, expect, it } from "vitest";
import { isAuthorized, readCreateJobBody, readCallbackBody } from "../src/http.ts";

const token = "a".repeat(48);
const env = { RUNNER_API_TOKEN: token };

describe("HTTP helpers", () => {
  it("bounds streaming callback data before decoding",async()=>{
    const request=new Request("https://runner.test",{method:"POST",body:new ReadableStream({start(controller){controller.enqueue(new Uint8Array(200));controller.close();}}),duplex:"half"} as RequestInit);
    const tooLarge=await readCallbackBody(request,100);expect(tooLarge).toBeInstanceOf(Response);expect((tooLarge as Response).status).toBe(413);
    const invalid=await readCallbackBody(new Request("https://runner.test",{method:"POST",body:new Uint8Array([255])}),100);expect((invalid as Response).status).toBe(400);
  });

  it("requires the configured bearer token", async () => {
    const valid = new Request("https://runner.test/v1/jobs", { headers: { authorization: `Bearer ${token}` } });
    const invalid = new Request("https://runner.test/v1/jobs", { headers: { authorization: "Bearer wrong" } });
    expect(await isAuthorized(valid, env)).toBe(true);
    expect(await isAuthorized(invalid, env)).toBe(false);
  });

  it("accepts no body or an empty object", async () => {
    expect(await readCreateJobBody(new Request("https://runner.test/v1/jobs", { method: "POST" }))).toEqual({});
    expect(await readCreateJobBody(new Request("https://runner.test/v1/jobs", { method: "POST", body: "" }))).toEqual({});
    expect(await readCreateJobBody(new Request("https://runner.test/v1/jobs", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }))).toEqual({});
  });

  it("rejects workflow overrides and malformed bodies", async () => {
    const override = await readCreateJobBody(new Request("https://runner.test/v1/jobs", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ prompt: "unexpected" }) }));
    expect(override).toBeInstanceOf(Response);
    expect((override as Response).status).toBe(400);
    const wrongType = await readCreateJobBody(new Request("https://runner.test/v1/jobs", { method: "POST", headers: { "content-type": "text/plain" }, body: "{}" }));
    expect((wrongType as Response).status).toBe(415);
  });
});
