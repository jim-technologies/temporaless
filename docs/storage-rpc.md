# Storage RPC

Yes: workflow, activity, timer, event, claim, and query storage should have protobuf RPC contracts.

The core durability contract is `temporaless.v1.RecordStoreService`. It is intentionally one service, not separate `ActivityStoreService`, `WorkflowStoreService`, and `TimerStoreService` definitions. Workflow replay needs these records together, and one cohesive service keeps generated clients small in Go and Python.

Cross-run search is separate: `temporaless.v1.RecordQueryService`. It is implemented by optional derived indexes, not by the core bucket store.

## Boundary

The domain-facing store interfaces still exist in each language:

- Go: `storage.Store` and `storage.ClaimStore`
- Python: `Store` and `ClaimStore` protocols

Those interfaces are the business-layer seam used by workflow replay. The RPC service is the cross-process and cross-language seam. A local OpenDAL store can be wrapped as a `RecordStoreService`, and a remote `RecordStoreService` can be wrapped back into the local store interface. Inspector listing and indexed retention use `QueryStore` / `RecordQueryService` instead.

## Implementations

- Go exposes a ConnectRPC handler and client-backed store in `adapters/go/connectstore`;
  v0.3.0 regenerated-stub parity for that package is a follow-up.
- Python exposes `temporaless.connectstore.RecordStoreService` (async), `ConnectStore` (async client), and `asgi_application` (mount on uvicorn/hypercorn/any ASGI server).
- The protobuf definitions and generated request/response types live under `api/temporaless/v1`.

## Core Surface

`RecordStoreService` exposes the point-operation surface the runtime needs:

- `GetStoreCapabilities`
- `GetWorkflow` / `PutWorkflow` / `GetLatestWorkflowRun` / `DeleteWorkflow` / `DeleteRun`
- `GetActivity` / `PutActivity` / `ListActivities` / `DeleteActivity`
- `GetTimer` / `PutTimer` / `ListTimers` / `DeleteTimer`
- `GetEvent` / `PutEvent` / `ListEvents` / `DeleteEvent`
- `GetClaim` / `TryCreateClaim` / `DeleteClaim`
- `DueTimers` (read the compact due-timer ledger)

Lists on `RecordStoreService` are run-scoped only. They exist for replay prefetch and run deletion, not for search. Deletions are idempotent.

## Query Surface

`RecordQueryService` is optional and implemented by a rebuildable index:

- `ListWorkflows`
- `ListActivities`
- `Sweep`
- `DueTimers`

Query RPCs provide status filters, ordering, pagination, indexed retention, and alternate indexed due-timer lookup. The bucket remains authoritative; query rows contain metadata only.

## Rules

- Use ConnectRPC stubs generated from protobuf.
- Store records as protobuf binary only.
- Keep record constants in protobuf enums.
- Report claim support with `GetStoreCapabilities`.
- Do not add Redis, SQL, or an always-on lock service to the core boundary.
- Keep schedulers separate; storage RPC is not a workflow control plane.
