# Working on Flow

Flow is a standalone Cloudflare deployment with one active configurable workflow. Preserve its TypeScript Worker and Go ACP supervisor boundary, all five harnesses, and authenticated operator API. Read README.md and the relevant implementation before changing lifecycle or deployment behavior.

## Sources and layout

- Edit committed `config/runner.yaml`, `catalog.yaml`, `workflow.yaml`, prompts, scripts and skills directly. No overlays or private configuration are needed for checks.
- Generate `src/generated-config.ts` and `.generated/wrangler.jsonc` with `pnpm config:generate`. Never hand-edit generated files or embed secrets in them.
- Worker entry is `src/index.ts`; routing is `api.ts`; lifecycle is `container.ts`; admission is `job-submission.ts`. Coordinators own capacity, schedules, budgets and OAuth cache.
- `internal/runner` is a package-main Go supervisor within the root module. Docker copies its production files into the build stage. Runtime config arrives through `RUNNER_CONFIG_JSON`.
- `history.ts` owns durable record identity, limits, indexing and R2 archival. CLI and E2E share `tools/flow-client.ts`.

## Invariants

- Public operator endpoints use bearer authentication and no-store responses. Internal callbacks require owning-job tokens; Slack requires signed requests and allowlisted actors.
- Real provider/API/OAuth secrets stay in Worker Secrets. Containers receive scoped placeholders. Keep credential origin/path matching, redirect rejection, bounded OAuth payloads and credential stripping intact.
- Plaintext runtime variables must be declared by name in the catalog; sanitize harness child environments. Automatic tool approval is intentional inside the non-root container.
- Every job pins its immutable bundle and trace identity. Idempotency must survive transport retries and never start duplicate work.
- Progress cannot overwrite cancelling, sleeping, resuming or suspended state. Release capacity after terminal persistence or durable cold suspension; reacquire before resuming.
- Human/budget waits pause aggregate execution time. Retention is distinct and must preserve resumable work until its exposed expiry.
- Snapshot memory with SQLite backup/quick-check, include WAL state, use conditional R2 writes, and update the loaded ETag. Successful resumed dispatch clears old snapshot metadata so final changes persist.
- Metric contracts count only durably accepted points. Preserve bounded retries/backpressure, stable record IDs/content hashes and rejection of conflicting identities. Later finished span snapshots can supersede earlier snapshots.
- Verbose history overflow marks truncation and drops counters without failing execution. Metrics and lifecycle/results have separate caps. Outbox overflow is retryable. Expiry deletes the entire job prefix and metadata, preserving bundles and workflow memory.
- Bound finalization and cancellation. Preserve results/errors when artifact or memory persistence fails; do not leave capacity held indefinitely.
- Support mixed and command-only workflows, including bundles with no prompts. Validate all bundle paths, hashes, totals, symlinks and archive contents; do not relax traversal checks.

## Profiles and deployment

Use explicit local/prod profiles. Never import credential files from another project or profile. Local and production resource identities are isolated. Local container callbacks use the configured public tunnel origin even when the API is accessed over loopback.

Do all local validation before remote writes. Keep 64 MiB resumable registry uploads and short-lived Wrangler credentials. Publish the active bundle after successful Worker deployment. Treat deployment/secrets/bundle sync as remote actions; ordinary code work does not imply a live deployment.

Keep dependencies and harness versions pinned. Changes to ACP adapters require the image probes and matching harness tests. Do not add unrelated services or a dashboard.

## Verification

Use Node 24, Go 1.26.2, pnpm and SQLite CLI. Run `pnpm check` and `go vet ./...` after substantive changes. For runtime/Docker/harness changes, run `pnpm docker:build` for linux/amd64. Live `pnpm e2e` requires a configured target; report clearly if it was not run.

Use meaningful fixtures independent of profile secrets. Exercise concurrency, retry, limits and failure paths for history/lifecycle changes. Avoid tests that just repeat implementation constants. Format Go with gofmt. Keep TypeScript strict. Document API/configuration changes and operational limitations in README.md. Never log credentials, submitted Slack content, or sensitive environment values.
