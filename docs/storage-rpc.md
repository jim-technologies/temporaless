# Storage RPC

Yes: workflow, activity, timer, event, claim, and query storage should have protobuf RPC contracts.

The core durability contract is `temporaless.v1.RecordStoreService`. It is intentionally one service, not separate `ActivityStoreService`, `WorkflowStoreService`, and `TimerStoreService` definitions. Workflow replay needs these records together, and one cohesive service keeps generated clients small in Go and Python.

Cross-run search is separate: `temporaless.v1.RecordQueryService`. It is implemented by optional derived indexes, not by the core bucket store.

## Boundary

`RecordStoreService` and `RecordQueryService` are the canonical adapter
contracts. Deployment decides how those service-shaped clients dispatch:

- local clients call the service implementation directly in-process, with no
  network hop
- remote clients use generated ConnectRPC/gRPC stubs
- OpenDAL-style bucket stores implement the point `RecordStoreService`
- SQL, DuckLake, or another rebuildable index implements production
  `RecordQueryService`

The domain-facing store interfaces still exist in each language:

- Go: `storage.Store`, `storage.ClaimStore`, and the bounded `storage.ClaimRunStore` deletion extension
- Python: `Store`, `ClaimStore`, and `ClaimRunStore` protocols

Those interfaces are the business-layer seam used by workflow replay. They
mirror the protobuf service semantics and are intentionally thin. Inspector
listing and indexed retention use `QueryStore` / `RecordQueryService` instead.

## Implementations

- Go exposes local and HTTP client-backed stores in `adapters/go/connectstore`:
  `NewLocalClientStore(...)` calls the generated service interface in-process,
  and `NewHTTPClientStore(...)` uses generated ConnectRPC clients.
  `NewHTTPHandler(...)` mounts only `RecordStoreService`;
  `NewHTTPHandlerWithLocalQuery(...)` explicitly adds the local/development
  query fallback over `storage.Store`; `NewHTTPHandlerWithQuery(...)` mounts a
  caller-supplied indexed `RecordQueryService`.
- Python exposes `temporaless.connectstore.RecordStoreService` (async),
  `ConnectStore.local(...)`, `ConnectStore.from_address(...)`,
  `ConnectQueryStore.local(...)`, `ConnectQueryStore.from_address(...)`, and
  `asgi_application` / `query_asgi_application` for ASGI servers.
- TypeScript exposes generated `temporaless.v1` types, `ConnectStore` and
  `ConnectQueryStore` wrappers, and a Node-only
  `@jim-technologies/temporaless/invariant` subpath that uses
  invariantprotocol to project `RecordStoreService` / `RecordQueryService`
  into MCP, CLI, HTTP/Connect, and descriptor-backed tool catalogs. It is not a
  workflow runtime.
- The protobuf definitions and generated request/response types live under `api/temporaless/v1`.

## Core Surface

`RecordStoreService` exposes the point-operation surface the runtime needs:

- `GetStoreCapabilities`
- `GetWorkflow` / `PutWorkflow` / `GetLatestWorkflowRun` / `DeleteWorkflow` / `DeleteRun`
- `GetActivity` / `PutActivity` / `ListActivities` / `DeleteActivity`
- `GetTimer` / `PutTimer` / `ListTimers` / `DeleteTimer`
- `GetEvent` / `PutEvent` / `ListEvents` / `DeleteEvent`
- `GetClaim` / `TryCreateClaim` / `DeleteClaim` / `ListClaims`
- `DueTimers` (read the compact due-timer ledger)

Lists on `RecordStoreService` are run-scoped only. They exist for replay prefetch and run deletion, not for search. `DeleteRun` snapshots and validates every listed record before deleting claims, then activities/timers/events/workflow; a separately configured claim store must implement `ClaimRunStore` or deletion is rejected before mutation. Deletions are idempotent.

`DeleteRun` is a bounded cleanup operation, not a transaction or execution fence. Quiesce the run before calling it. A claim created concurrently after the listing snapshot can survive this pass; strict concurrent deletion would require a run tombstone checked by every claim create, which the create-only core does not pretend to provide.

## Query Surface

`RecordQueryService` is optional and implemented by a rebuildable index:

- `ListWorkflows`
- `ListActivities`
- `Sweep`
- `DueTimers`

Query RPCs provide status filters, ordering, pagination, indexed retention, and alternate indexed due-timer lookup. The bucket remains authoritative; query rows contain metadata only.

`RecordQueryService.Sweep` follows the same bounded deletion rules for every
eligible run: preflight claim capability, require `ClaimRunStore` from a
claim-capable backend, snapshot and validate all run-scoped claims and records,
then delete claims before records. `NO_CLAIMS` remains record-only. Sweep is
nontransactional and is not an execution fence, so the operator must externally
quiesce eligible runs while retention executes.

Production inspectors, large operational search, and exact retention sweeps
should use an indexed `RecordQueryService` backed by SQL, DuckLake, or another
rebuildable metadata index. Bucket/local query fallbacks are only for small
local deployments and development.

## Rules

- Use ConnectRPC stubs generated from protobuf.
- Store records as protobuf binary only.
- Keep record constants in protobuf enums.
- Report claim support with `GetStoreCapabilities`.
- Do not add Redis, SQL, or an always-on lock service to the core boundary.
- Keep schedulers separate; storage RPC is not a workflow control plane.
