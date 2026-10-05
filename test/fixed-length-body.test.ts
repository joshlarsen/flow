import { describe, expect, it, vi } from "vitest";
import { writeFixedLengthBody, type FixedLengthStreamFactory } from "../src/fixed-length-body.ts";

function testStreamFactory(lengths: number[]): FixedLengthStreamFactory {
  return (length) => {
    lengths.push(length);
    return new TransformStream<Uint8Array, Uint8Array>();
  };
}

describe("fixed-length streaming uploads", () => {
  it("transfers the complete body without cross-stream pipeTo", async () => {
    const lengths: number[] = [];
    const bytes = new TextEncoder().encode("sqlite snapshot");
    const source = new Response(bytes).body!;
    source.pipeTo = vi.fn(() => {
      throw new TypeError("Inter-TransformStream ReadableStream.pipeTo() is not implemented.");
    });
    const write = vi.fn(async (body: ReadableStream<Uint8Array>) => new Uint8Array(await new Response(body).arrayBuffer()));

    const uploaded = await writeFixedLengthBody(source, bytes.byteLength, write, () => true, testStreamFactory(lengths));

    expect(lengths).toEqual([bytes.byteLength]);
    expect(uploaded).toEqual(bytes);
    expect(write).toHaveBeenCalledOnce();
    expect(source.pipeTo).not.toHaveBeenCalled();
  });

  it("cancels an unread source when a conditional write is rejected", async () => {
    let cancelled = false;
    const source = new ReadableStream<Uint8Array>({
      pull(controller) {
        controller.enqueue(new Uint8Array(64));
      },
      cancel() {
        cancelled = true;
      },
    });

    const result = await writeFixedLengthBody(source, 1024, async () => null, (value) => value !== null, testStreamFactory([]));

    expect(result).toBeNull();
    expect(cancelled).toBe(true);
  });
});
