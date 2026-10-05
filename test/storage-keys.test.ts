import { describe, expect, it } from "vitest";
import {
  activeWorkflowKey,
  jobOutputKey,
  jobOutputPrefix,
  workflowBundleArchiveKey,
  workflowBundleManifestKey,
  workflowMemoryKey,
} from "../src/storage-keys.ts";

describe("R2 storage keys", () => {
  it("uses bucket-root resource paths without a deployment prefix", () => {
    const digest = "a".repeat(64);
    const sortKey = "20260914T130000.000Z";
    expect(activeWorkflowKey).toBe("active.json");
    expect(workflowBundleArchiveKey(sortKey, digest)).toBe(`bundles/${sortKey}-${digest.slice(0, 12)}/bundle.tgz`);
    expect(workflowBundleManifestKey(sortKey, digest)).toBe(`bundles/${sortKey}-${digest.slice(0, 12)}/manifest.json`);
    expect(workflowMemoryKey("default")).toBe("memory/default.sqlite3");
    expect(jobOutputPrefix("job-id")).toBe("jobs/job-id/output/");
    expect(jobOutputKey("job-id", "reports/result.json")).toBe("jobs/job-id/output/reports/result.json");
  });
});
