export const activeWorkflowKey = "active.json";

/** Returns the immutable directory shared by one bundle archive and manifest. */
export function workflowBundlePrefix(sortKey: string, digest: string): string {
  return `bundles/${sortKey}-${digest.slice(0, 12)}`;
}

export function workflowBundleArchiveKey(sortKey: string, digest: string): string {
  return `${workflowBundlePrefix(sortKey, digest)}/bundle.tgz`;
}

export function workflowBundleManifestKey(sortKey: string, digest: string): string {
  return `${workflowBundlePrefix(sortKey, digest)}/manifest.json`;
}

export function workflowMemoryKey(workflowName: string): string {
  return `memory/${workflowName}.sqlite3`;
}

export function jobOutputPrefix(jobId: string): string {
  return `jobs/${jobId}/output/`;
}

export function jobOutputKey(jobId: string, relativePath: string): string {
  return `${jobOutputPrefix(jobId)}${relativePath}`;
}

export function jobCheckpointKey(jobId: string): string {
  return `jobs/${jobId}/checkpoint/session.tgz`;
}
