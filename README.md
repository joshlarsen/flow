# Flow

Flow deploys one reusable agent workflow into a Cloudflare Worker and Cloudflare Containers. Submit a run through a bearer-authenticated API or the CLI, poll its status, inspect events, traces and numeric metrics, and download output artifacts. The deployment owns its R2 storage and Durable Objects.

Each deployment has one active workflow. API submissions and optional UTC schedules execute the same pinned bundle. Steps run serially in a shared workspace: commands execute directly and agent steps use Pi, Codex, Claude Code, OpenCode or Grok through ACP. The shipped workflow runs a tool check, then asks Pi with OpenAI to write `output/report.md` and emit `report.completed=1`.

## Requirements

- Node.js 24+, pnpm (the package pins its version), Go 1.26.2, Git and tar.
- Docker with Buildx and `linux/amd64` support. Cloudflare Containers runs the amd64 image, including on an ARM development host.
- SQLite CLI (`sqlite3`) for Go memory tests; it is installed in the runtime image.
- `crane` for deployment image uploads. Install with `go install github.com/google/go-containerregistry/cmd/crane@latest` and put your Go bin directory on PATH.
- A Cloudflare account with Workers, Containers and R2 enabled; Wrangler authentication authorized to manage these resources. Run `pnpm exec wrangler login`, or provide an appropriately scoped `CLOUDFLARE_API_TOKEN` to commands. Account permissions must cover Worker scripts/secrets, Containers/registry, Durable Objects, R2 and, for development, Tunnels.
- An OpenAI API key with access to the model selected in `config/catalog.yaml`. Model access and identifiers vary by account; edit the catalog before running if necessary.

## Start from a clean checkout

```sh
pnpm install --frozen-lockfile
pnpm config:generate
pnpm check
pnpm env:local
```

`env:local` creates `.env`, `.env.profiles/local/.env.secrets`, and `.env.profiles/local/.env.cloudflare` from committed examples and selects local in `.env.active`. Fill those files before using Cloudflare. Create the production directory from the corresponding examples when preparing production:

```sh
mkdir -p .env.profiles/prod
cp config/env.secrets.example .env.profiles/prod/.env.secrets
cp config/env.prod.cloudflare.example .env.profiles/prod/.env.cloudflare
chmod 600 .env.profiles/prod/.env.secrets
```

Use `openssl rand -hex 32` to generate `RUNNER_API_TOKEN`. Put it and `OPENAI_API_KEY` in the selected `.env.secrets`; never commit them. Put your 32-character Cloudflare account ID and public `RUNNER_URL` in `.env.cloudflare`. `RUNNER_URL` is an origin without a path, query, fragment or userinfo, for example `https://flow.example.workers.dev`. It is the callback address used by the container even when you submit through localhost. Local execution requires the stable public HTTPS tunnel origin, since a container cannot call your host's loopback address.

`.env` contains shared **plaintext** runtime variables declared in `catalog.yaml` under `environment`. Unlisted variables are not forwarded to the agent. Do not define a variable in multiple environment files. Process environment overrides file values; clear inherited credentials when switching accounts or profiles. Code generation and tests work before selecting a profile. Remote commands require an explicit profile and never import root-level credential files.

## Local execution

Local and production use isolated resources: defaults are `flow-local` / `flow-local-storage` and `flow` / `flow-storage`. Local development still uses remote R2 and an authenticated tunnel.

1. Set local account ID, API token, provider key, `CLOUDFLARE_TUNNEL_NAME` and public `RUNNER_URL`.
2. Run `pnpm bundle:sync` to create the local bucket and upload/activate the workflow bundle.
3. Run `pnpm dev`. The tool inspects the named tunnel and creates it only if Wrangler reports it absent. Configure its stable hostname as `RUNNER_URL`.
4. In another shell, run `pnpm flow preflight`, then `pnpm flow run --wait --json`. `--url http://127.0.0.1:8787` can address the local Worker while callbacks continue to use the configured public origin.

`pnpm dev -- --port 8788` passes supported options to Wrangler. Use `pnpm env:status` to inspect the active resource names and file paths without displaying secrets.

## Production deployment

Fill production profile files, then run:

```sh
pnpm env:prod
```

This selects production, validates configuration and required local values, synchronizes secrets, builds/uploads the container, deploys the Worker, activates the uploaded bundle, and reconciles budget policy. It performs remote writes. `pnpm secrets:sync` and `pnpm deploy` are available separately and require production to be active. When all provider keys are present except the API token, secret synchronization can generate and save a bearer token.

The deploy command validates local configuration and executable prerequisites before uploading. Large image blobs use resumable 64 MiB registry chunks with short-lived Wrangler credentials; crane completes the image upload. The active workflow changes only after Worker deployment succeeds. If deployment fails, fix the reported problem and retry; uploaded immutable bundles may remain unused. Secrets, image, Worker and bundle publication are separate operations rather than one transaction. After changing only workflow resources, `pnpm bundle:sync` publishes a new bundle to the active profile. It reconciles token budgets when the API is reachable.

Do not switch production/local by renaming resources manually. Switch profiles, check `env:status`, regenerate, and use the profile-specific commands.

## Configure the workflow

All editable sources are committed:

| Path | Purpose |
| --- | --- |
| `config/runner.yaml` | Deployment identity, capacity, security and storage limits |
| `config/catalog.yaml` | Harnesses, providers, model aliases, credentials, routes, plaintext variable names |
| `config/workflow.yaml` | Ordered steps, defaults, aggregate timeout, schedules, memory and budget policy |
| `config/prompts/` | UTF-8 Markdown agent prompts |
| `config/scripts/` | Bundled scripts; command steps may also invoke image tools |
| `config/skills/` | Bundled agent skills, installed under `.agents/skills/` |
| `src/` | Worker API, coordinators, lifecycle, history and credential proxy |
| `internal/runner/` | Go supervisor and ACP harness execution |
| `tools/` | Code generation, profiles, deployment, client and CLI |

`pnpm config:generate` produces ignored `src/generated-config.ts` and `.generated/wrangler.jsonc`. Edit YAML, then regenerate; generated files are not configuration sources. There are no configuration overlays. `pnpm config:check` validates YAML, references and workflow policy. `pnpm bundle:sync` also validates actual prompt/script/skill content and sizes.

Workflow example:

```yaml
version: 1
name: default
memory: false
workflow_timeout: 1h
schedules: []
defaults:
  step_timeout: 15m
  harness: pi
  provider: openai
  model: gpt-5.6-luna
  reasoning_effort: medium
steps:
  - id: prepare
    command: [bash, scripts/prepare.sh]
    timeout: 30s
  - id: report
    prompt: report.md
    allow_user_input: false
    required_metrics:
      - report:
          - completed: Number of reports written
```

Step IDs are unique. Prompt paths are relative to `config/prompts/`; agent steps may override each default independently. Commands are argument arrays, not shell strings; use `[bash, scripts/task.sh]` for shell logic. Commands may appear anywhere, including a workflow containing only commands and no prompts. Each step timeout must fit within the workflow timeout. The aggregate execution clock pauses during human-input and quota waits; retention is a separate deadline.

Supported reasoning values are `none`, `minimal`, `low`, `medium`, `high`, `xhigh`, and `max`; harness/model support may narrow those values. `null` clears an inherited reasoning setting. Agent completion with `max_tokens`, `max_turn_requests` or `refusal` produces `partial` and stops subsequent steps. Missing final output or invalid ACP responses fail the step. All harnesses use automatic tool approval in the isolated container and run as UID 10001.

The image pins ACP adapters and includes Bash, Git, curl, wget, jq, ripgrep, fd, SQLite, GitHub CLI, gog, uv/uvx, Python 3.14 and Node 24. Pins and adapter checks are in `Dockerfile`.

### Providers and credentials

| Harness | Supported provider protocols |
| --- | --- |
| Pi | OpenAI Responses, Anthropic, xAI, OpenAI-compatible |
| Codex | OpenAI Responses |
| Claude Code | Anthropic |
| OpenCode | OpenAI Responses, Anthropic, OpenAI-compatible |
| Grok | xAI |

Models are aliases with provider-specific identifiers. Add provider definitions and model mappings directly to the catalog. A Cloudflare AI endpoint uses `{ kind: cloudflare-ai }` and requires an account ID during generation. Direct URL endpoints require HTTPS. No unused provider key is required: secret discovery selects workflow providers, every configured credential route, enabled interaction secrets, and the API token.

Credentials stay in Worker Secrets. Agent processes receive scoped placeholders; outbound HTTPS interception replaces matching authorization headers. Matching respects origin and path boundaries, rejects redirects for credential acquisition, and strips credential copies. Requests without scoped credentials can use ordinary internet access; scoped credentials cannot be forwarded to unmatched hosts. Credentials do not belong in prompts, plaintext environment variables, generated JSON, command arguments or logs.

A credential-backed API route can use:

```yaml
credentials:
  service:
    source: { env: SERVICE_AUTH, header: Authorization, value_prefix: "Bearer " }
    upstream: { header: Authorization, secret: SERVICE_TOKEN, value_prefix: "Bearer " }
routes:
  service:
    url_prefix: https://api.example.com/v1
    credential: service
```

`SERVICE_AUTH` exposes a scoped placeholder to commands. `SERVICE_TOKEN` lives in Worker Secrets. For Basic auth set both prefixes to `"Basic "` and store raw `username:password` as the secret.

OAuth upstream definitions support client credentials or refresh tokens:

```yaml
upstream:
  header: Authorization
  value_prefix: "Bearer "
  oauth:
    token_url: https://identity.example.com/oauth/token
    client_id_secret: SERVICE_CLIENT_ID
    client_auth: { method: client_secret_basic, secret: SERVICE_CLIENT_SECRET }
    grant: { type: client_credentials }
    scopes: [read]
```

For refresh tokens use `grant: { type: refresh_token, refresh_token_secret: SERVICE_REFRESH_TOKEN }`. Client auth can be `client_secret_basic`, `client_secret_post`, or `none` where supported. Access tokens are cached in the credential broker with expiry skew and bounded acquisition. Rotating refresh tokens are rejected. For Google refresh-token bootstrap, use `pnpm oauth:google:bootstrap -- --credential <catalog-name> --account <email>` after configuring a matching refresh-token credential.

### Metrics and artifacts

Declare numeric contracts under `required_metrics`. The runner appends deterministic emission instructions to each agent prompt. The `metrics.emit` MCP tool accepts `{"metrics":["report.completed=1"]}`; commands can use `flow-metric report.completed=1`. Names use lowercase namespaces/keys, values must be finite. A metric counts as emitted only after durable Worker acceptance. A missing required metric fails its step. Token usage is reported separately from explicit numeric metrics.

Write output files under `/workspace/output`. Paths in the artifact API are relative to that directory (`report.md`, not `output/report.md`). Artifacts are scanned after execution, bounded, hashed and uploaded. Symlinks and unsafe paths are rejected. Download requires the owning job's recorded artifact entry. Finalization retries transient persistence errors for up to `memory.persistence_timeout` (default two minutes), then reports an error and releases capacity.

### Persistent SQLite memory

Set `memory: true` and keep `container.max_instances: 1`. The workflow owns `memory/<workflow-name>.sqlite3` in R2. The runtime database is `/workspace/memory.sqlite3`, exposed as `AGENT_MEMORY_DB`. First execution creates a checked database; later runs load it. Completion and cold suspension produce standalone WAL-aware snapshots and conditional R2 writes. Conflicting writes fail explicitly. Resumed execution creates a fresh terminal snapshot. Memory persists beyond job retention; back it up separately and remove it explicitly when resetting a workflow.

### UTC schedules and token budgets

Add schedules such as:

```yaml
schedules:
  - id: weekday-morning
    cron: "0 13 * * MON-FRI"
token_budget: { limit: 1_000_000, period: day }
```

Schedules use native Cloudflare Cron expressions and UTC. Each occurrence pins the active bundle. Capacity-blocked occurrences coalesce to the newest pending occurrence per schedule; retries preserve identity. Schedules do not promise every missed occurrence will execute.

Token quotas are shared by workflow name, reset at UTC day/week boundaries, and charge completed agent-step usage idempotently. They suspend between successful steps, save checkpoints and release container capacity. Oldest suspended jobs have priority on reset. Unknown token usage fails closed. Command steps do not charge tokens. Update bundle policy and call `POST /v1/workflow-budget/reconcile` to wake eligible jobs promptly. A CLI wait timeout does not cancel suspended jobs.

### Slack input

Set `interactions.provider: slack` and supply `live_wait`, `response_ttl`, request/response limits, checkpoint limits, `team_id`, `conversation_id`, `allowed_user_ids`, `bot_token_secret` and `signing_secret`. Use the same shape as the interaction schema in `tools/config.ts`; default recommended values are 30s live wait, 24h response TTL, 65536-byte request/response limits, and checkpoint limits of 10000 files / 128 MiB per file / 256 MiB total.

Run `pnpm slack:manifest` to generate a secret-free app manifest for the active public HTTPS origin. Install the app, invite its bot into the configured conversation, and store bot/signing credentials in the profile. Enable `allow_user_input: true` only for Codex, Claude Code or Grok steps. Pi and OpenCode do not support these native form requests.

Signed Slack actions enforce timestamp, team, conversation and allowed-user checks. Form responses resume the same ACP session. If no answer arrives within the live wait, the runner checkpoints, sleeps and releases capacity. A later answer reacquires capacity and restores the session; expired requests are declined/cancelled according to the interaction state. Resume attempts are bounded. Submitted content is not written into provider feedback logs.

## API and CLI

Every `/v1` operator endpoint requires `Authorization: Bearer <RUNNER_API_TOKEN>` and returns `Cache-Control: no-store`. Slack uses its signed callback protocol. Internal runner callbacks use per-job tokens.

| Method / path | Behavior |
| --- | --- |
| `GET /v1/preflight` | Validate active bundle, selected secrets, callback config and R2 read/write access without starting an agent |
| `POST /v1/jobs` | Start the active workflow; empty body or `{}` only, at most 1024 bytes; returns 202 |
| `GET /v1/jobs` | Retained jobs, newest first |
| `GET /v1/jobs/:id` | Status, steps, usage, errors, history state and artifact metadata |
| `DELETE /v1/jobs/:id` | Request cancellation; finalization may continue briefly |
| `GET /v1/jobs/:id/events` | Ordered normalized event records |
| `GET /v1/jobs/:id/traces` | Portable span records, latest completed snapshot per span |
| `GET /v1/jobs/:id/metrics` | Numeric metric records |
| `GET /v1/jobs/:id/artifacts/:path` | Download a recorded artifact |
| `POST /v1/workflow-budget/reconcile` | Refresh active budget policy and wake eligible work |

Lists return `{items,next_cursor}`. Use `limit` (default 100, maximum 200) and the opaque `cursor` from the prior response. Pages describe live retained state, not a frozen snapshot. Idempotent submissions use a 1–128 character `Idempotency-Key`; the same key and workflow digest resolve to the same retained job, and a changed digest conflicts. Capacity exhaustion returns 429; unavailable configuration/storage returns 503. Expired jobs return 404.

```sh
pnpm flow preflight
pnpm flow run --idempotency-key daily-report --wait --json
pnpm flow list --limit 20 --json
pnpm flow get JOB_ID
pnpm flow wait JOB_ID --timeout 600
pnpm flow cancel JOB_ID
pnpm flow events JOB_ID --limit 100 --json
pnpm flow traces JOB_ID --json
pnpm flow metrics JOB_ID --json
pnpm flow artifact-download JOB_ID report.md --output report.md
```

Artifact downloads refuse to overwrite an existing local file. `--cursor`, `--limit`, `--url`, `--timeout` (seconds), and `--json` support scripting. Run/wait exit 2 for a terminal outcome other than success; request/validation errors exit 1. Wait expiry leaves the job running, sleeping or suspended.

```sh
curl -H "Authorization: Bearer $RUNNER_API_TOKEN" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-1' \
  -d '{}' https://flow.example.workers.dev/v1/jobs
```

## Storage and retention

R2 stores `active.json`, immutable `bundles/<timestamp>-<digest>/`, workflow `memory/`, and per-job `jobs/<id>/output/`, `checkpoint/`, and `history/{events,traces,metrics}/`. SQLite-backed Durable Objects hold execution state, capacity/idempotency indexes, schedule queues, budgets, OAuth cache and the durable history index/outbox.

History records have stable identities and content hashes. Exact retries deduplicate; changed event/metric IDs conflict. A resumed span may replace an earlier finished snapshot with a later finish while preserving its identity. Callback acceptance means the record is durable in the job object; R2 writes retry independently. Retrieval includes accepted records while archival is pending.

Default history limits are 25 MiB for verbose events/spans, 5 MiB for metrics, 1 MiB for lifecycle/results, and 8 MiB of pending callback outbox data, with a separate lifecycle reservation for Worker-owned terminal records. `history.truncated`, `dropped_events` and `dropped_traces` report verbose overflow; execution continues and metrics, artifacts and terminal outcomes are retained separately. Metrics over their cap are rejected and cannot satisfy a required metric contract. A full outbox applies retryable backpressure. Persistent callback failure becomes a job error; infrastructure loss can still interrupt execution.

Jobs default to 24-hour retention from admission. Input and budget waits can extend expiry to preserve resumability. Expiration removes the entire `jobs/<id>/` prefix and job state; bundle and workflow memory objects remain. Job views expose `expires_at`, pending archival count and last error. Retention deletion retries on storage failure.

## Validation and troubleshooting

```sh
pnpm check
go vet ./...
pnpm docker:build
pnpm e2e
```

`check` validates/generates configuration, typechecks, runs Worker tests and Go tests. `docker:build` checks the production architecture image and pinned adapter probes. `e2e` submits the active workflow, checks that it matches local resources, waits for completion/history archival, validates token usage for agent workflows and memory metadata when enabled. It requires a configured running target and incurs execution costs.

For callback failures, verify the public `RUNNER_URL` and tunnel reachability. For readiness failures, inspect the missing-secret/bundle/R2 error before submitting. For model failures, check provider protocol and account model access. For stuck jobs, inspect current status, `resume_at`, pending input, budget state and `history.last_error`; use cancellation to stop active work. Use Wrangler logs for infrastructure details and authenticated history endpoints for run records. Do not print real credentials during troubleshooting.
