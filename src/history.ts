import { createHash } from "node:crypto";

export type HistoryKind = "events" | "traces" | "metrics";
type RecordValue = Record<string, unknown>;
interface HistoryRow extends Record<string, SqlStorageValue> {
  kind: string;
  id: string;
  sequence: number;
  payload: string;
  hash: string;
  bytes: number;
  archived: number;
  category: string;
}
export class HistoryError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value && typeof value === "object")
    return `{${Object.entries(value)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, v]) => `${JSON.stringify(key)}:${canonical(v)}`)
      .join(",")}}`;
  return JSON.stringify(value);
}
const hash = (value: string) =>
  createHash("sha256").update(value).digest("hex");
export function initializeHistory(sql: SqlStorage): void {
  sql.exec(`CREATE TABLE IF NOT EXISTS history_records (kind TEXT, id TEXT, sequence INTEGER, payload TEXT, hash TEXT, bytes INTEGER, archived INTEGER DEFAULT 0, category TEXT, PRIMARY KEY(kind,id));
    CREATE TABLE IF NOT EXISTS history_dropped (kind TEXT, id TEXT, hash TEXT, PRIMARY KEY(kind,id));
    CREATE TABLE IF NOT EXISTS history_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), dropped_events INTEGER DEFAULT 0, dropped_traces INTEGER DEFAULT 0);
    INSERT OR IGNORE INTO history_meta(singleton) VALUES (1);`);
}
/** Validates identities and all limits before committing a callback atomically. */
export function appendHistory(
  sql: SqlStorage,
  payload: string,
  limits: {
    maxVerboseBytes: number;
    maxMetricBytes: number;
    maxLifecycleBytes: number;
    maxPendingBytes: number;
  },
): string | null {
  const batch = JSON.parse(payload) as RecordValue;
  const inserted: Record<HistoryKind, RecordValue[]> = {
    events: [],
    traces: [],
    metrics: [],
  };
  const sizes = Object.fromEntries(
    sql
      .exec<{ category: string; bytes: number }>(
        "SELECT category, SUM(bytes) AS bytes FROM history_records GROUP BY category",
      )
      .toArray()
      .map((r) => [r.category, r.bytes]),
  );
  let pending = sql
    .exec<{
      bytes: number;
    }>(
      "SELECT COALESCE(SUM(length(CAST(payload AS BLOB))),0) AS bytes FROM event_outbox",
    )
    .one().bytes;
  const staged: HistoryRow[] = [],
    dropped: { kind: HistoryKind; id: string; hash: string }[] = [];
  const identities = new Map<string, HistoryRow>();
  for (const [kind, field] of [
    ["events", "events"],
    ["traces", "spans"],
    ["metrics", "metrics"],
  ] as const) {
    if (!Array.isArray(batch[field]))
      throw new HistoryError(400, `Missing ${field}`);
    for (const value of batch[field] as RecordValue[]) {
      if (!value || typeof value !== "object" || Array.isArray(value))
        throw new HistoryError(400, "Invalid history record");
      const id =
        value[
          kind === "events"
            ? "event_id"
            : kind === "traces"
              ? "span_id"
              : "metric_id"
        ];
      if (
        typeof id !== "string" ||
        !id.length ||
        id.length > 256 ||
        /[\u0000-\u001f]/.test(id)
      )
        throw new HistoryError(400, "Invalid record identity");
      const body = canonical(value),
        digest = hash(body),
        bytes = new TextEncoder().encode(body).length;
      const identity = `${kind}:${id}`;
      const existing =
        identities.get(identity) ??
        sql
          .exec<HistoryRow>(
            "SELECT * FROM history_records WHERE kind=? AND id=?",
            kind,
            id,
          )
          .toArray()[0];
      if (existing) {
        if (existing.hash === digest) continue;
        if (kind !== "traces")
          throw new HistoryError(
            409,
            "Record identity has conflicting content",
          );
        const prior = JSON.parse(existing.payload) as RecordValue;
        if (
          prior.trace_id !== value.trace_id ||
          prior.started_at !== value.started_at ||
          prior.parent_span_id !== value.parent_span_id ||
          prior.name !== value.name
        )
          throw new HistoryError(409, "Span identity has conflicting content");
        if (typeof value.finished_at !== "string")
          throw new HistoryError(400, "Invalid span finish");
        if (value.finished_at === prior.finished_at)
          throw new HistoryError(409, "Span snapshot has conflicting content");
        if (
          typeof prior.finished_at === "string" &&
          value.finished_at < prior.finished_at
        )
          continue;
      }
      const oldDrop =
        sql
          .exec<{
            hash: string;
          }>("SELECT hash FROM history_dropped WHERE kind=? AND id=?", kind, id)
          .toArray()[0] ?? dropped.find((r) => r.kind === kind && r.id === id);
      if (oldDrop) {
        if (oldDrop.hash !== digest)
          throw new HistoryError(
            409,
            "Dropped record identity has conflicting content",
          );
        continue;
      }
      const category =
        kind === "metrics"
          ? "metric"
          : (kind === "events" &&
                typeof value.type === "string" &&
                /^(run\.|artifact\.|workflow\.memory\.)/.test(value.type)) ||
              (kind === "traces" && value.parent_span_id === null)
            ? "lifecycle"
            : "verbose";
      const cap =
        category === "metric"
          ? limits.maxMetricBytes
          : category === "lifecycle"
            ? limits.maxLifecycleBytes
            : limits.maxVerboseBytes;
      const nextSize = (sizes[category] ?? 0) + bytes - (existing?.bytes ?? 0);
      if (nextSize > cap) {
        if (category !== "verbose")
          throw new HistoryError(413, `${category} history limit exceeded`);
        dropped.push({ kind, id, hash: digest });
        continue;
      }
      sizes[category] = nextSize;
      pending += bytes;
      if (pending > limits.maxPendingBytes)
        throw new HistoryError(503, "History outbox is full; retry later");
      const record: HistoryRow = {
        kind,
        id,
        sequence:
          kind !== "traces"
            ? Number(value.sequence)
            : Date.parse(
                String(
                  value.started_at ??
                    value.occurred_at ??
                    value.timestamp ??
                    new Date().toISOString(),
                ),
              ),
        payload: body,
        hash: digest,
        bytes,
        archived: 0,
        category,
      };
      identities.set(identity, record);
      staged.push(record);
      inserted[kind].push(value);
    }
  }
  const filtered = JSON.stringify({
    ...batch,
    events: inserted.events,
    spans: inserted.traces,
    metrics: inserted.metrics,
  });
  const currentPending = sql
    .exec<{
      bytes: number;
    }>(
      "SELECT COALESCE(SUM(length(CAST(payload AS BLOB))),0) AS bytes FROM event_outbox",
    )
    .one().bytes;
  if (
    staged.length &&
    currentPending + new TextEncoder().encode(filtered).length >
      limits.maxPendingBytes
  )
    throw new HistoryError(503, "History outbox is full; retry later");
  // No await occurs between validation and writes: the Durable Object serializes this transaction.
  for (const row of staged)
    sql.exec(
      "INSERT OR REPLACE INTO history_records(kind,id,sequence,payload,hash,bytes,archived,category) VALUES (?,?,?,?,?,?,0,?)",
      row.kind,
      row.id,
      Number.isFinite(row.sequence) ? row.sequence : 0,
      row.payload,
      row.hash,
      row.bytes,
      row.category,
    );
  for (const row of dropped) {
    sql.exec(
      "INSERT INTO history_dropped(kind,id,hash) VALUES (?,?,?)",
      row.kind,
      row.id,
      row.hash,
    );
    sql.exec(
      `UPDATE history_meta SET ${row.kind === "events" ? "dropped_events" : "dropped_traces"} = ${row.kind === "events" ? "dropped_events" : "dropped_traces"} + 1 WHERE singleton=1`,
    );
  }
  if (!staged.length) return null;
  return filtered;
}
export async function archiveHistory(
  bucket: R2Bucket,
  sql: SqlStorage,
  jobId: string,
  payload: string,
): Promise<void> {
  const batch = JSON.parse(payload) as RecordValue;
  for (const [kind, field] of [
    ["events", "events"],
    ["traces", "spans"],
    ["metrics", "metrics"],
  ] as const)
    for (const value of batch[field] as RecordValue[]) {
      const id = String(
        value[
          kind === "events"
            ? "event_id"
            : kind === "traces"
              ? "span_id"
              : "metric_id"
        ],
      );
      const latest = sql
        .exec<HistoryRow>(
          "SELECT * FROM history_records WHERE kind=? AND id=?",
          kind,
          id,
        )
        .one();
      const body = latest.payload,
        digest = latest.hash;
      await bucket.put(
        `jobs/${jobId}/history/${kind}/${encodeURIComponent(id)}.json`,
        body,
        {
          httpMetadata: { contentType: "application/json" },
          customMetadata: { sha256: digest },
        },
      );
      sql.exec(
        "UPDATE history_records SET archived=1 WHERE kind=? AND id=? AND hash=?",
        kind,
        id,
        digest,
      );
    }
}
export function historyView(
  sql: SqlStorage,
  lastError: string | null,
  updatedAt: string | null,
) {
  const counts = sql
    .exec<{
      pending: number;
      total: number;
    }>(
      "SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN archived=0 THEN 1 ELSE 0 END),0) AS pending FROM history_records",
    )
    .one();
  const dropped = sql
    .exec<{
      dropped_events: number;
      dropped_traces: number;
    }>(
      "SELECT dropped_events,dropped_traces FROM history_meta WHERE singleton=1",
    )
    .one();
  return {
    state: counts.pending
      ? lastError
        ? ("retrying" as const)
        : ("pending" as const)
      : ("archived" as const),
    truncated: dropped.dropped_events + dropped.dropped_traces > 0,
    ...dropped,
    pending_records: counts.pending,
    retained_records: counts.total,
    last_error: lastError,
    updated_at: updatedAt,
  };
}
export function historyPage(
  sql: SqlStorage,
  kind: HistoryKind,
  limit: number,
  cursor: string | null,
) {
  let position: { sequence: number; id: string } | null = null;
  if (cursor) {
    try {
      position = JSON.parse(atob(cursor));
    } catch {
      throw new HistoryError(400, "Invalid cursor");
    }
    if (
      !position ||
      !Number.isSafeInteger(position.sequence) ||
      typeof position.id !== "string"
    )
      throw new HistoryError(400, "Invalid cursor");
  }
  const rows = position
    ? sql
        .exec<HistoryRow>(
          "SELECT * FROM history_records WHERE kind=? AND (sequence>? OR (sequence=? AND id>?)) ORDER BY sequence,id LIMIT ?",
          kind,
          position.sequence,
          position.sequence,
          position.id,
          limit + 1,
        )
        .toArray()
    : sql
        .exec<HistoryRow>(
          "SELECT * FROM history_records WHERE kind=? ORDER BY sequence,id LIMIT ?",
          kind,
          limit + 1,
        )
        .toArray();
  const items = rows.slice(0, limit),
    last = items.at(-1);
  return {
    items: items.map((r) => JSON.parse(r.payload)),
    next_cursor:
      rows.length > limit && last
        ? btoa(JSON.stringify({ sequence: last.sequence, id: last.id }))
        : null,
  };
}
