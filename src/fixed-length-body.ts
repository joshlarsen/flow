export interface FixedLengthStreamPair {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export type FixedLengthStreamFactory = (length: number) => FixedLengthStreamPair;

function cloudflareFixedLengthStream(length: number): FixedLengthStreamPair {
  const stream = new FixedLengthStream(length);
  return {
    readable: stream.readable,
    writable: stream.writable as WritableStream<Uint8Array>,
  };
}

interface BodyTransfer {
  readonly done: Promise<void>;
  cancel(reason?: unknown): Promise<void>;
}

/** Pumps bytes without pipeTo, which workerd cannot use across container stream implementations. */
function transferBody(source: ReadableStream<Uint8Array>, destination: WritableStream<Uint8Array>): BodyTransfer {
  const reader = source.getReader();
  const writer = destination.getWriter();
  const cancel = async (reason?: unknown): Promise<void> => {
    await Promise.allSettled([
      Promise.resolve().then(() => reader.cancel(reason)),
      Promise.resolve().then(() => writer.abort(reason)),
    ]);
  };
  const done = (async () => {
    try {
      while (true) {
        const chunk = await reader.read();
        if (chunk.done) break;
        await writer.write(chunk.value);
      }
      await writer.close();
    } catch (error) {
      await cancel(error);
      throw error;
    } finally {
      reader.releaseLock();
      writer.releaseLock();
    }
  })();
  return { done, cancel };
}

/** Gives a streaming consumer an exact-length body and drains or cancels its source. */
export async function writeFixedLengthBody<T>(
  source: ReadableStream<Uint8Array>,
  length: number,
  write: (body: ReadableStream<Uint8Array>) => Promise<T>,
  accepted: (result: T) => boolean = () => true,
  createStream: FixedLengthStreamFactory = cloudflareFixedLengthStream,
): Promise<T> {
  const stream = createStream(length);
  const transfer = transferBody(source, stream.writable);
  try {
    const result = await write(stream.readable);
    if (!accepted(result)) {
      await transfer.cancel();
      await transfer.done.catch(() => undefined);
      return result;
    }
    await transfer.done;
    return result;
  } catch (error) {
    await transfer.cancel(error);
    await transfer.done.catch(() => undefined);
    throw error;
  }
}
