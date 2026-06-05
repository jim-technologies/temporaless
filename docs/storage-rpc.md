# Storage RPC

Yes: workflow, activity, timer, and claim storage should have a protobuf RPC contract.

The contract is `temporaless.v1.RecordStoreService`. It is intentionally one service, not separate `ActivityStoreService`, `WorkflowStoreService`, and `TimerStoreService` definitions. Workflow replay needs these records together, and one cohesive service keeps generated clients small in Go and Python.

## Boundary

The domain-facing store interfaces still exist in each language:

- Go: `storage.Store` and `storage.ClaimStore`
- Python: `Store` and `ClaimStore` protocols

Those interfaces are the business-layer seam used by workflow replay. The RPC service is the cross-process and cross-language seam. A local OpenDAL store can be wrapped as a `RecordStoreService`, and a remote `RecordStoreService` can be wrapped back into the local store interface.

## Implementations

- Go exposes a ConnectRPC handler and client-backed store in `adapters/go/connectstore`.
- Python exposes `temporaless.connectstore.RecordStoreService` (async), `ConnectStore` (async client), and `asgi_application` (mount on uvicorn/hypercorn/any ASGI server).
- The protobuf definitions and generated request/response types live under `api/temporaless/v1`.

## Surface

`RecordStoreService` exposes the full CRUD-plus-list surface for every record kind so any transport (CLI, gRPC, HTTP, MCP) can wrap it without language-local glue:

- `GetStoreCapabilities`
- `GetWorkflow` / `PutWorkflow` / `ListWorkflows` / `DeleteWorkflow`
- `GetActivity` / `PutActivity` / `ListActivities` / `DeleteActivity`
- `GetTimer` / `PutTimer` / `ListTimers` / `DeleteTimer`
- `GetEvent` / `PutEvent` / `ListEvents` / `DeleteEvent`
- `GetClaim` / `TryCreateClaim`
- `Sweep` (delete COMPLETED runs older than `max_age` — operator retention RPC; janitor adapter is a thin wrapper)
- `DueTimers` (find SCHEDULED timers under IN_PROGRESS workflows whose `fire_at <= now` — timer scanner is a thin wrapper)

Listings filter by `WorkflowKey` scope plus an optional status; deletions are idempotent (return `deleted: false` if the record was already gone). Claims intentionally have no delete RPC in V1 — they are write-once for create-only adapters; CAS-capable adapters can extend later. `Sweep` and `DueTimers` are server-side compound operations: clients pay one round-trip; the server does the list-and-act loop next to the storage backend (huge latency win when the RPC server is colocated with S3/GCS).

## Rules

- Use ConnectRPC stubs generated from protobuf.
- Store records as protobuf binary only.
- Keep record constants in protobuf enums.
- Report claim support with `GetStoreCapabilities`.
- Do not add Redis, SQL, or an always-on lock service to this boundary.
- Keep schedulers separate; storage RPC is not a workflow control plane.
