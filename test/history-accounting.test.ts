import { describe, expect, it } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { recordReceivedEventSequenceSQL } from "../src/history-accounting.ts";
describe("history sequence persistence", () => {
  it("advances the persisted sequence without regressing on late batches", () => {
    const database = new DatabaseSync(":memory:");
    database.exec("CREATE TABLE job (singleton INTEGER PRIMARY KEY, event_sequence INTEGER NOT NULL); INSERT INTO job VALUES (1, 69);");

    database.prepare(recordReceivedEventSequenceSQL).run(74);
    database.prepare(recordReceivedEventSequenceSQL).run(72);

    expect(database.prepare("SELECT event_sequence FROM job").get()).toEqual({ event_sequence: 74 });
    database.close();
  });

});
