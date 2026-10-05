import { DatabaseSync } from "node:sqlite";
import { describe, expect, it, vi } from "vitest";
import {
  appendHistory,
  archiveHistory,
  historyPage,
  historyView,
  initializeHistory,
} from "../src/history.ts";
function storage() {
  const db = new DatabaseSync(":memory:");
  const sql = {
    exec(query: string, ...args: unknown[]) {
      if (!args.length && query.includes(";")) {
        db.exec(query);
        return { toArray: () => [], one: () => ({}) };
      }
      const statement = db.prepare(query);
      if (!/^\s*SELECT/i.test(query)) {
        statement.run(...(args as never[]));
        return { toArray: () => [], one: () => ({}) };
      }
      const rows = statement.all(...(args as never[]));
      return { toArray: () => rows, one: () => rows[0] };
    },
  } as unknown as SqlStorage;
  sql.exec("CREATE TABLE event_outbox(id TEXT PRIMARY KEY,payload TEXT)");
  initializeHistory(sql);
  return { db, sql };
}
const limits = {
  maxVerboseBytes: 10000,
  maxMetricBytes: 10000,
  maxLifecycleBytes: 10000,
  maxPendingBytes: 100000,
};
const event = (id: string, sequence = 1, type = "session/update") => ({
  event_id: id,
  sequence,
  type,
  occurred_at: "2026-10-04T00:00:00Z",
  data: { text: "hello" },
});
const batch = (
  events: unknown[] = [],
  spans: unknown[] = [],
  metrics: unknown[] = [],
) =>
  JSON.stringify({
    schema_version: 4,
    run: { source_run_id: "job" },
    events,
    spans,
    metrics,
  });
describe("durable run history", () => {
  it("deduplicates reordered JSON and rejects changed identities without partial writes", () => {
    const { sql } = storage();
    appendHistory(sql, batch([event("one")]), limits);
    expect(
      appendHistory(
        sql,
        batch([
          {
            data: { text: "hello" },
            occurred_at: "2026-10-04T00:00:00Z",
            type: "session/update",
            sequence: 1,
            event_id: "one",
          },
        ]),
        limits,
      ),
    ).toBeNull();
    expect(() =>
      appendHistory(
        sql,
        batch([
          event("two", 2),
          { ...event("one"), data: { text: "changed" } },
        ]),
        limits,
      ),
    ).toThrow(/conflicting/);
    expect(historyPage(sql, "events", 100, null).items).toHaveLength(1);
  });
  it("archives with hashes and survives partial R2 failures and retries", async () => {
    const { sql } = storage();
    const payload = appendHistory(
      sql,
      batch([event("one"), event("two", 2)]),
      limits,
    )!;
    const objects = new Map<string, string>();
    let fail = true;
    const bucket = {
      put: vi.fn(
        async (
          key: string,
          body: string,
          options: { customMetadata: { sha256: string } },
        ) => {
          expect(options.customMetadata.sha256).toMatch(/^[a-f0-9]{64}$/);
          JSON.parse(body);
          if (key.includes("two") && fail) throw new Error("R2 unavailable");
          objects.set(key, body);
        },
      ),
    } as unknown as R2Bucket;
    await expect(archiveHistory(bucket, sql, "job", payload)).rejects.toThrow(
      /unavailable/,
    );
    expect(historyView(sql, "R2 unavailable", null).pending_records).toBe(1);
    fail = false;
    await archiveHistory(bucket, sql, "job", payload);
    expect(objects.size).toBe(2);
    expect(historyView(sql, null, null).state).toBe("archived");
  });
  it("truncates verbose history while retaining metrics, artifacts and terminal outcomes", () => {
    const { sql } = storage();
    const small = { ...limits, maxVerboseBytes: 1 };
    appendHistory(sql, batch([event("dropped")]), small);
    expect(appendHistory(sql, batch([event("dropped")]), small)).toBeNull();
    appendHistory(
      sql,
      batch(
        [
          event("terminal", 3, "run.finished"),
          event("artifact", 2, "artifact.created"),
        ],
        [],
        [{ metric_id: "metric", sequence: 1, value: 1 }],
      ),
      small,
    );
    expect(historyView(sql, null, null)).toMatchObject({
      truncated: true,
      dropped_events: 1,
      retained_records: 3,
    });
  });
  it("rejects metrics and pending byte overflow before committing any records", () => {
    const { sql } = storage();
    expect(() =>
      appendHistory(sql, batch([], [], [{ metric_id: "metric", value: 1 }]), {
        ...limits,
        maxMetricBytes: 1,
      }),
    ).toThrow(/metric/);
    expect(() =>
      appendHistory(sql, batch([event("one")]), {
        ...limits,
        maxPendingBytes: 1,
      }),
    ).toThrow(/outbox/);
    expect(historyPage(sql, "events", 100, null).items).toHaveLength(0);
  });
  it("keeps the latest logical span on resumed updates and delayed archival", async () => {
    const { sql } = storage();
    const span = {
      span_id: "span",
      trace_id: "trace",
      parent_span_id: null,
      name: "workflow.run",
      started_at: "2026-10-04T00:00:00Z",
      finished_at: "2026-10-04T01:00:00Z",
    };
    const first = appendHistory(sql, batch([], [span]), limits)!;
    const latest = { ...span, finished_at: "2026-10-04T02:00:00Z" };
    appendHistory(sql, batch([], [latest]), limits);
    const put = vi.fn(
      async (_key: string, _body: string, _options: unknown) => undefined,
    );
    await archiveHistory({ put } as unknown as R2Bucket, sql, "job", first);
    expect(JSON.parse(put.mock.calls[0]![1] as string)).toEqual(latest);
    expect(appendHistory(sql, batch([], [span]), limits)).toBeNull();
    expect(() =>
      appendHistory(sql, batch([], [{ ...latest, name: "different" }]), limits),
    ).toThrow(/conflicting/);
  });
  it("paginates ordered events without duplicates and rejects invalid cursors", () => {
    const { sql } = storage();
    appendHistory(
      sql,
      batch([event("three", 3), event("one", 1), event("two", 2)]),
      limits,
    );
    const first = historyPage(sql, "events", 2, null);
    expect(first.items.map((item) => item.sequence)).toEqual([1, 2]);
    expect(
      historyPage(sql, "events", 2, first.next_cursor).items.map(
        (item) => item.sequence,
      ),
    ).toEqual([3]);
    expect(() => historyPage(sql, "events", 2, "invalid")).toThrow(/cursor/);
  });
});
