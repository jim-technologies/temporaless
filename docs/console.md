# Read-only console

Core Temporaless ships no UI, and nothing in it depends on one: the records in
the bucket are the source of truth. `cmd/temporaless-console` is an optional,
read-only operator console that projects those records into an executions
view. It holds no state, writes nothing, and can scale to zero.

## What it shows

One page, top to bottom:

- **Workflows**: the latest run of every workflow ID in a namespace (from the
  latest-run pointers), with status, workflow type, start and duration, and
  what an unfinished run is waiting on. Filter by status.
- **Runs of** the selected workflow ID.
- **Run**: the selected run's summary, its pending state, its history, a
  per-boundary table (one row per activity, timer, event, and claim), its
  rendered inputs and results, and the whole `DescribeRun` response.
- **Scheduled wakes**: the due ledger, with overdue wakes flagged and any
  disagreement with the canonical timer reported (never repaired).

It shows only what records evidence. An unfinished run is labelled
retrying, sleeping, polling, executing (a live claim), overdue wake, stale
claim, or waiting without a durable wake; see
[run inspection](inspection.md) for the derivation. History is derived from
record timestamps. Temporaless keeps point records, not a journal, so
overwritten intermediate states and released claims do not appear, and the
page says so.

There are no operator actions (reset, deliver an event, re-run). Use the
[runbook](runbook.md) procedures and the [operator CLI](operator-cli.md).

## How it is built

```text
browser ──► temporaless-console (Go, stateless)
            ├─ GET  /, /assets/*      embedded UI (Vite host, built with Node at build time)
            ├─ GET  /ui/config        title, auth mode, and the executions template; no secrets
            ├─ POST /<service>/<method>  Connect/HTTP  ┐ bearer authentication
            ├─ POST /mcp                 MCP          ┘ on every call
            ├─ gRPC (optional listener)
            └─ GET  /healthz, /readyz
                     │
                     ▼  Invariant Protocol server, request validation
            temporaless.v1.RunInspectionService   (adapters/go/inspection)
            TerminalService facade: Get, ListSources (adapters/go/console)
                     │
                     ▼  bounded listings and point reads, read-only credentials
            OpenDAL fs or s3 record store
```

- [`adapters/go/inspection`](../adapters/go/inspection) implements
  `RunInspectionService` over the bucket.
- [`adapters/go/console`](../adapters/go/console) projects it onto the public
  terminal-core dashboard contract (vendored under `third_party/`), bundles
  the `executions.json` template, and holds the authentication, scoping, and
  page-token code.
- `cmd/temporaless-console` reads the configuration, opens the stores, and
  serves. Its UI host (`cmd/temporaless-console/ui`) only renders the
  template; every source and payload is decided server-side.

Any host that speaks the dashboard contract can render the same template
against `TerminalService` instead of the embedded UI.

## Run it locally

```sh
flox activate -- make build-console   # UI + binary at build/temporaless-console
cat > console.yaml <<'EOF'
listen: 127.0.0.1:8080
authentication: {loopback: {}}
stores:
  - id: local
    displayName: Local records
    namespaces: [default]
    filesystem: {root: /path/to/records}
EOF
build/temporaless-console -config console.yaml
```

Open <http://127.0.0.1:8080>. `-check` validates the configuration, opens
every store, and exits. For UI work, `npm run dev` in
`cmd/temporaless-console/ui` proxies the API to a running console
(`CONSOLE_URL`, default `http://127.0.0.1:8080`).

## Configuration

The schema is
[`adapters/go/console/proto/temporaless/console/v1/config.proto`](../adapters/go/console/proto/temporaless/console/v1/config.proto);
the file is YAML or JSON with ProtoJSON field names. Unknown fields are
rejected, so a typo cannot drop a security setting. Credentials are never
inline: every secret is the path of a mounted file.

```yaml
listen: 0.0.0.0:8080
title: Executions
pageTokenKeyFile: /etc/temporaless-console/page-token-key   # at least 32 bytes
authentication:
  jwt:
    issuer: https://issuer.example.com
    audience: temporaless-console
    jwksUrl: https://issuer.example.com/.well-known/jwks.json
    workspaceClaim: workspace_id        # the tenant claim; default workspace_id
  openfga:
    apiUrl: https://openfga.example.com
    storeId: 01HXXXXXXXXXXXXXXXXXXXXXXX
stores:
  - id: engine
    displayName: Data engine
    namespaces: [default]
    workspace: ws_example               # the tenant that owns this store root
    payloadRelation: can_read           # can_write limits payload values to editors
    overdueGrace: 120s
    payloadDescriptorsFile: /etc/temporaless-console/engine.binpb   # optional
    s3:
      bucket: state
      root: workflows/
      endpoint: https://s3.example.com
      region: auto
      accessKeyIdFile: /etc/temporaless-console/s3-access-key-id
      secretAccessKeyFile: /etc/temporaless-console/s3-secret-access-key
limits: {defaultPageSize: 50, maxPageSize: 100}
```

| Setting | Meaning |
|---|---|
| `stores[].namespaces` | The only namespaces a caller may read. An empty namespace is always rejected. |
| `stores[].filesystem` / `stores[].s3` | Where the `temporaless/v2` tree lives. Give the console read-only credentials. |
| `stores[].listClaims` | Read run-scoped claims from the bucket (only when claims live there). Without it a run reports that claims were not inspected. |
| `stores[].payloadDescriptorsFile` | A `FileDescriptorSet` for rendering application payloads as JSON. Well-known types always render; anything else stays opaque bytes. |
| `limits` | Page size (default 50, max 100), objects a filtered page reads (500), run directories listed per workflow (20,000; beyond that the call refuses and asks for an index), and records read per kind per run (2,000; beyond that the run is marked truncated). |
| `grpcListen` | Optional native gRPC listener. Not allowed in loopback mode. |

## Authentication and tenancy

| Mode | For | Behaviour |
|---|---|---|
| `loopback` | Development | No authentication. The listen address must be loopback. |
| `staticToken` | A private network | One bearer token from a mounted file (at least 16 bytes); sees every store. The UI asks for the token and keeps it in the tab's memory only. |
| `jwt` + `openfga` | Hosted, multi-tenant | ES256/RS256 JWTs verified against a JWKS URL or file (issuer, audience, expiry, 30 s leeway; subject and tenant claims configurable). A store is visible only when its `workspace` equals the token's tenant and OpenFGA allows `user:<sub> <readRelation> workspace:<workspace>`; payload values also need `payloadRelation`. Decisions are cached for 30 s, and an OpenFGA outage fails closed. |

Tenancy is one store per tenant root: point each tenant's store at its own
bucket prefix and name the tenant in `workspace`. Scope is always taken from
the verified token, never from the request. The relation and object type names
are configurable (`authentication.userType`, `authentication.workspaceType`,
`stores[].readRelation`).

Listing continuation tokens are sealed with HMAC-SHA256 over the method,
store, namespace, filters, the caller's tenant, and an expiry, so a token
cannot be replayed under another scope.

## Deploy it

- **Image.** `docker build --target console .` builds a distroless image
  that runs as uid 65532 and reads `/etc/temporaless-console/console.yaml`.
  Mount credentials as files that the configuration names.
- **Writable `/tmp`.** OpenDAL's Go binding unpacks one native library per
  storage service into `TMPDIR` at start, and the console deletes them once
  its stores are open. Nothing else is written. With a read-only root
  filesystem, mount a small in-memory `emptyDir` or `tmpfs` at `/tmp`.
- **Probes.** `GET /healthz` and `GET /readyz`. Neither touches the bucket,
  so waking from zero replicas does not wait on storage.
- **Scale to zero.** The process is stateless and starts in about a quarter
  of a second (below), so an HTTP scaler with a minimum of zero replicas
  fits. The first request waits for the scaler to start a pod.
- **Credentials.** Read-only bucket credentials. The console cannot write,
  but read-only credentials make that true even if it had a bug.

### S3 support and cold start

The S3 service ships as the module `github.com/apache/opendal-go-services/s3`
(v0.1.16, the same release as the `fs` service). Both services embed their
native library for Linux amd64 and arm64 only. Even with `CGO_ENABLED=0` the
binary is dynamically linked, because it loads that library through the
system loader and libffi. The runtime image therefore needs glibc, libffi,
and libgcc (the `console` target copies them in), and a static-only base
image will not run it.

Measured on 2026-09-25 on an x86-64 workstation, with a stripped
`CGO_ENABLED=0` build (31.6 MB), against a local SeaweedFS 4.44 S3 endpoint
using credentials that may only list and read:

| Step | Time |
|---|---|
| Process start to operators ready (`-check`: validate, unpack and load the fs and s3 libraries, build the operator) | ~0.21 s |
| Process start to `/healthz` answering | ~0.22 s |
| Process start to the first `ListWorkflowDirectory` page (8 workflows) | ~0.25 s |
| First `DescribeRun` (4 activities, 12 history events, 8 payloads) | 24 ms |
| First `ListScheduledWakes` | 27 ms |
| Warm `ListWorkflowDirectory` | 25–28 ms |

The resident set was about 73 MB, `TMPDIR` was empty once the stores were
open, and an unauthenticated call got 401. The endpoint was local, so
network latency is excluded; on a real bucket, add its round-trip times.

## Dashboard framework versions

The template renders with terminal-core v0.5.2. The contract under
`third_party/medallion-terminal-core` and the UI's
`medallion-terminal-core` dependency must come from the same commit, and
`scripts/check_versions.py` enforces it. That contract has no generic JSON
payload, so the full `DescribeRun` response is shown as an object view with
one JSON property per response field. When a newer terminal-core release adds
a JSON payload case, move the vendored contract and the UI pin together, run
`make generate`, and switch that panel to the JSON widget.

## Not in this version

- Global search across every run (filter by status, type, or time). It needs
  the optional query index; bucket-native inspection reports
  `indexed_search=false`.
- A timeline (Gantt) view of a run, and the approved `WorkflowPlan` graph.
- Operator actions.
