# Temporaless Agent Instructions

This repository is building an opinionated serverless workflow framework. Keep the framework small, explicit, and convention-driven.

## Core Rule

Every workflow and every activity must follow the same shape:

- exactly one protobuf request message
- exactly one protobuf response message
- errors are returned or raised normally
- protobuf binary is the only framework storage format

Go functions should look like protobuf RPC handlers:

```go
func(ctx context.Context, req *marketdatav1.FetchRequest) (*marketdatav1.FetchResponse, error)
```

Python functions should follow the same convention:

```python
def fetch(req: FetchRequest) -> FetchResponse:
    ...
```

Do not add arbitrary args, kwargs, custom codecs, JSON payloads, or generic object serialization.

Existing unary protobuf RPC handlers may be wrapped as workflows or activities. Keep one options-driven wrapper per boundary. Do not add fixed/dynamic wrapper variants; optional resolver fields belong inside the wrapper options. Keep these wrappers transport-agnostic; ConnectRPC belongs at the boundary, not inside workflow replay logic.

## Architecture

- Repository layout follows a language namespace:
  - `api/`: protobuf API definitions only
  - `core/go/`: Go core runtime and generated protobuf packages
  - `core/py/`: Python core runtime, generated protobuf packages, and uv project
  - `adapters/go/`: Go adapters
  - `adapters/py/`: Python adapters
  - `examples/go/` and `examples/py/`: language-specific examples
- The core owns workflow/activity replay, protobuf records, and blessed storage infrastructure.
- Inside core, workflow packages are the business layer and storage packages are infrastructure. OpenDAL-backed point stores may live in core storage.
- Workflow and activity wrappers live in the core business layer because they only adapt the unary protobuf function shape into replay semantics. Keep this API thin and opinionated.
- Adapters live next to the core when they adapt external systems such as ConnectRPC, Temporal, scheduler indexes, or narrow backend-specific claims.
- Temporal compatibility adapters must use real Temporal SDKs and must cover retries, activity timeouts, and durable sleeps before claiming support. Keep Temporal SDK dependencies out of core packages.
- Core decisions should be conservative and opinionated. Reject ambiguous behavior instead of inventing defaults.
- Python is **async-only end-to-end**. Workflow bodies, activity bodies, `wrap_workflow` / `wrap_activity`, `temporaless.workflow.run` / `Workflow.execute_activity` (the public activity API; `run_activity` is the lower-level primitive used by `execute_activity` — prefer `execute_activity` in user code) / `Workflow.sleep` / `Workflow.wait_event`, the `Store` Protocol surface, `OpenDALStore` (built on `opendal.AsyncOperator`), `ConnectStore` and the `RecordStoreService` ASGI app, `cronscheduler.Scheduler.tick` + `last_fire(s)_from_runs`, `inspector.*`, `janitor.sweep`, `timerscanner.due_timers`, and `send_event` all use `async def`. Sync callables and sync stores are rejected at the boundary. Reasoning: the framework's stated workloads (LLM, Twitter, stocks/HTTP, ML) are I/O-bound, modern Python is async-first, and a hybrid sync-storage / async-everything-else design caused real impedance whenever an adapter wanted to do parallel I/O. The serving surface is ConnectRPC ASGI (`asgi_application`); WSGI is gone.
- Go stays sync (no `async/await` primitive in Go; goroutines + sync function signatures are idiomatic).
- Temporaless is a generic workflow framework, not a 1-to-1 Temporal replacement. Temporal-flavored knobs (activity timeouts, heartbeats, schedule-to-close, sticky task queues, signal-channel select, workflow-level retry policy) must NOT live in the core. If a caller needs them, they belong in a Temporal-compat adapter. Generic options that exist in Dagster, Prefect, and other orchestrators (retry policy, durable sleep, events, claims, annotations) are fine in core.
- Adapters must either prove compatibility with the source system or document each semantic decision and rejection in the adapter package.
- Storage is durable and distributed by default. Prefer OpenDAL-backed stores. Avoid in-memory storage in framework APIs, examples, and tests.
- Core storage is point-operation only: deterministic GET/PUT/DELETE by protobuf key, run-scoped listing for replay prefetch and run deletion, create-if-absent claims, latest-run pointers, and the due-timer ledger. Cross-run search, inspector listing, status filtering, and indexed retention belong in optional query adapters, never in the core bucket store.
- ConnectRPC is the transport layer whenever a protobuf RPC boundary is needed.
- Go and Python are the only first-class languages.

## Versioning

The repository-root `VERSION` is the release version for every Temporaless SDK
and adapter. Ecosystem manifests and lockfiles are checked mirrors; update them
together with `make version-set VERSION=X.Y.Z`. Releases use one plain root tag
`vX.Y.Z`. Do not create language-, directory-, SDK-, or adapter-specific
version streams or tags. Invariant Protocol remains a separately versioned
external project.

## Distribution

Temporaless-owned Go, Python, TypeScript, and Rust packages are distributed
only from Git. Do not publish them to, or document installation from, PyPI,
the npm registry, crates.io, or another language registry. Production installs
must pin one immutable commit SHA; release-oriented examples may use the one
root `vX.Y.Z` tag. Language-specific subdirectories and package names are
source/package locators only, never separate release identities.

Keep registry publication disabled in ecosystem metadata wherever the
ecosystem supports it (`private: true` for npm and `publish = false` for
Cargo). This restriction applies to Temporaless-owned packages; third-party
dependencies may still resolve from their normal registries.

## Storage

Stored records are protobuf binary files at deterministic v2 flat keys:

```text
temporaless/v2/{namespace}/{workflow_id}/{run_id}/workflow.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/activity/{activity_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/timer/{timer_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/event/{event_id}.binpb
temporaless/v2/{namespace}/{workflow_id}/{run_id}/claim/{claim_id}.binpb
```

Keys are constructed from protobuf key fields and must not be parsed back into identity in runtime code. If code needs an id, read the protobuf payload. The only v1 path parser lives in the one-shot migration tool.

IDs may contain only ASCII letters, numbers, `.`, `_`, `-`, `:`, and `=`. Slashes are rejected because object keys are path-like. Namespace and workflow_id values beginning with `_` are reserved for Temporaless system prefixes such as `_latest` and `_due`; do not use them for application records. Do not add charset rules solely to support path parsing.

Do not generate workflow IDs, run IDs, activity IDs, timer IDs, or claim owner IDs inside the framework. The caller must provide them explicitly. Temporaless may validate storage-safe characters for path components, but it must not choose an ID scheme for the application.

## Protobuf

The project is still designing `temporaless.v1`. It is fine to reshape v1 messages and field numbering while the framework is pre-release. Do not add legacy compatibility paths unless explicitly requested.

Use Buf for protobuf formatting, linting, and generation.

Use Protovalidate for protobuf-defined validation. Cross-language runtime option types, including workflow and activity options, must live in `temporaless.v1` instead of being redefined separately in Go and Python. Wrapper structs may stay language-local only when they contain non-protobuf values such as stores, functions, or callables.

Framework constants that affect records, RPCs, or cross-language behavior must be protobuf enums or protobuf messages. Examples: record schema versions, timer kinds, claim resource types, and claim capabilities. Avoid parallel handwritten string constants in Go and Python.

Point storage RPC contracts belong in `temporaless.v1.RecordStoreService`. Cross-run query RPC contracts belong in `temporaless.v1.RecordQueryService`, implemented only by optional derived indexes. Language storage interfaces may remain as small infrastructure seams, but they should mirror generated protobuf request/response semantics and use generated record, key, enum, and option types.

When adding point storage RPC code, keep one cohesive `RecordStoreService` instead of separate workflow/activity/timer RPC services. Provide a thin service wrapper for local stores and a thin client-backed store for generated clients. Do not add SQL imports or database requirements to core packages.

## Development Environment

Flox owns the development environment. Keep it thin:

- `go`
- `python314`
- `uv`
- `buf`
- `libffi`
- `gcc-unwrapped` `lib` output

Do not add language-specific libraries, linters, or generators to Flox if they can live in `go.mod`, `uv.lock`, or `buf.gen.yaml`.

## Testing

Prefer table-driven tests.

In Go, structure tests as `tests := []struct { ... }{... }` with `t.Run(test.name, ...)`.

In Python, prefer `pytest.mark.parametrize` or compact case lists when testing multiple variants.

Tests should use OpenDAL `fs` with a temporary directory rather than memory stores. This keeps the mental model distributed and stateless while still being fast locally.

## Timers And Scheduling

Durable sleep is a core primitive. It must write protobuf timer records and return a typed pending error instead of blocking a process.

Cron is an adapter concern. Cron should create workflow runs with protobuf inputs. SQL may be introduced as an optional scheduler index, but the core must not require SQL.

## Claims And Leases

Claims should use storage-native conditional writes. Do not add Redis or another always-on lock server to the core.

Claim owner IDs are caller-provided. Do not generate random owner IDs or read implicit owner IDs from the environment.

Claim adapters must declare their generated `temporaless.v1.ClaimCapability`: `CLAIM_CAPABILITY_NO_CLAIMS`, `CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`, or `CLAIM_CAPABILITY_CAS_CLAIMS`.

Do not implement check-then-write locking. If a backend cannot atomically create or compare-and-swap a claim object, the framework should proceed idempotently or return a typed unsupported/busy result from an adapter.

Backend-specific GCS/S3 adapters may implement stronger claims using generation or ETag preconditions.

Python OpenDAL can use `if_not_exists=True` for create-only claims. Go OpenDAL currently must not claim generic support unless conditional writes are exposed by the binding.

GoCDK may be used only for the narrow claims adapter, because its blob API exposes `WriterOptions.IfNotExist` and maps that to native object-store preconditions in supported drivers. Do not replace the default OpenDAL record store with GoCDK.

## Code Style

- Keep functions direct. Avoid long chains of tiny pass-through helpers.
- Copying a little code is acceptable when it keeps behavior obvious.
- Avoid configuration knobs unless they are required by the framework convention.
- Use generated protobuf types and deterministic protobuf serialization for digests.
- Run `flox activate -- scripts/check` before handing work back.
