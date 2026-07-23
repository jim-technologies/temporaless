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

- Go: point `storage.Store`, optional conditional `storage.EventDeliveryStore`,
  `storage.ClaimStore`, bounded `storage.ClaimRunStore`, and optional
  `storage.QueryStore`
- Python: point `Store`, conditional `EventDeliveryStore`, `ClaimStore`,
  `ClaimRunStore`, and optional `QueryStore` protocols

Those interfaces are the business-layer seam used by workflow replay. They
mirror the protobuf service semantics and are intentionally thin. Inspector
listing and indexed retention use `QueryStore` / `RecordQueryService` instead.

## Implementations

- Go exposes local and HTTP client-backed stores in `adapters/go/connectstore`:
  `NewLocalClientStore(...)` calls the generated service interface in-process,
  and `NewHTTPClientStore(...)` uses generated ConnectRPC clients.
  `NewHTTPHandler(...)` mounts only `RecordStoreService`;
  `NewHTTPHandlerWithLocalQuery(store, query, ...)` explicitly adds a supplied
  `storage.QueryStore`; `NewHTTPHandlerWithQuery(...)` mounts a caller-supplied
  `RecordQueryService`. The optional `adapters/go/scanquery` implementation is
  for offline/development bucket scans only.
- Python exposes `temporaless.connectstore.RecordStoreService` (async),
  `ConnectStore.local(...)`, `ConnectStore.from_address(...)`,
  `ConnectQueryStore.local(...)`, `ConnectQueryStore.from_address(...)`, and
  `asgi_application` / `query_asgi_application` for ASGI servers.
- TypeScript exposes generated `temporaless.v1` types, `ConnectStore` and
  `ConnectQueryStore` wrappers, and a Node-only
  `@jim-technologies/temporaless/invariant` subpath that uses
  invariantprotocol to project `RecordStoreService` / `RecordQueryService`
  into MCP, CLI, HTTP/Connect, and descriptor-backed tool catalogs. Its default
  catalog is read-only; mutations, point-store timer repair, retention sweeps,
  and deletes require `includeOperatorMethods: true` plus an authenticated,
  least-privilege operator boundary. This option filters those projections; it
  does not authorize a native gRPC server created from the same Invariant
  object. Native gRPC requires separate authentication and per-RPC
  authorization. It is not a workflow runtime.
- The protobuf definitions and generated request/response types live under `api/temporaless/v1`.

## Core Surface

`RecordStoreService` exposes the point-operation surface the runtime needs:

- `GetStoreCapabilities`
- `GetWorkflow` / `PutWorkflow` / `GetLatestWorkflowRun` / `DeleteWorkflow` / `DeleteRun`
- `GetActivity` / `PutActivity` / `ListActivities` / `DeleteActivity`
- `GetTimer` / `PutTimer` / `ListTimers` / `DeleteTimer`
- `GetEvent` / `PutEvent` / `DeliverEvent` / `ListEvents` / `DeleteEvent`
- `GetClaim` / `TryCreateClaim` / `DeleteClaim` / `ListClaims`
- `DueTimers` (read the compact due-timer ledger)

Lists on `RecordStoreService` are run-scoped only. They exist for replay prefetch and run deletion, not for search. These lists are unpaginated and materialize the selected run; core `DueTimers` likewise materializes its selected namespace. Keep runs and namespaces bounded, partition timer-heavy tenants, and use an optional indexed due query or external scheduler for very large backlogs. `DeleteRun` snapshots and validates every listed record before deleting claims, then activities/timers/events/workflow; a separately configured claim store must implement `ClaimRunStore` or deletion is rejected before mutation. Deletions are idempotent.

Treat both storage services as privileged internal APIs with per-method
authorization. A namespace is a storage partition, not an authorization
boundary, and an empty namespace on `DueTimers` intentionally scans every
application namespace. Remote deployments must authenticate the transport and
authorize each RPC with a ConnectRPC interceptor or equivalent gateway policy;
do not expose these handlers directly to untrusted workflow users.

The bearer-token production examples represent one trusted internal principal
with access to their mounted surface. Production should issue separate
least-privilege identities to workflow runtimes, event senders, and operators.
An event sender normally receives only `DeliverEvent`; restrict `PutEvent`,
reset, delete, sweep, claim cleanup, and point-store timer repair RPCs to the
operator identity.

`DeleteRun` is a bounded cleanup operation, not a transaction or execution fence. Quiesce the run before calling it. A claim created concurrently after the listing snapshot can survive this pass; strict concurrent deletion would require a run tombstone checked by every claim create, which the create-only core does not pretend to provide.

## Event Delivery

`DeliverEvent` is the application-facing create-once boundary used by
`SendEvent` / `send_event`. The first payload for an `EventKey` wins. A retry
with the same deterministic protobuf `Any` payload returns
`EVENT_DELIVERY_DISPOSITION_IDEMPOTENT` and retains the original
`received_at`; a different payload fails with a typed conflict. A backend that
cannot atomically create-if-absent reports
`EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE` and rejects delivery. Adapters must
never substitute a check-then-write sequence.

`PutEvent` deliberately has replace semantics. Keep it behind an operator
boundary for migrations, repairs, and fixtures; webhook and approval handlers
should use `DeliverEvent`.

Python `OpenDALStore` advertises create-only delivery only when the selected
operator exposes `write_with_if_not_exists`. The current Go OpenDAL binding
exposes only unconditional writes, so direct Go `OpenDALStore` reports no
atomic event delivery. A Go application can deliver through a `ConnectStore`
whose remote service advertises create-only delivery, or through a narrow
native conditional-write `EventDeliveryStore`. ConnectStore transports the
capability; it does not manufacture atomicity.

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
rebuildable metadata index. The Go `scanquery` adapter is an explicit
offline/development fallback and never expands the core bucket interface.

The bundled `cmd/temporaless` binary is another offline/development fallback:
it registers only OpenDAL `fs`. For cloud stores, use authenticated
ConnectStore/RecordQueryService clients or generated remote operator tooling
instead of placing cloud credentials in that local CLI.

## Rules

- Use ConnectRPC stubs generated from protobuf.
- Store records as protobuf binary only.
- Keep record constants in protobuf enums.
- Report claim and event-delivery support with `GetStoreCapabilities`.
- Do not add Redis, SQL, or an always-on lock service to the core boundary.
- Keep schedulers separate; storage RPC is not a workflow control plane.
