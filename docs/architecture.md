# Architecture

## Goal

Temporaless provides a storage-backed workflow replay model for serverless jobs. The first target workloads are market-data workflows: pulling stocks, crypto, prediction market, price, and social data, normalizing it, and storing it into databases used for trading and analysis.

The design optimizes for:

- simple workflow code in Go and Python
- protobuf as the storage format
- OpenDAL as the storage API
- ConnectRPC for protobuf transport when a storage or execution boundary becomes remote
- convention over configuration

## Repository Layout

The repository is organized by product boundary first, then language:

- `api/`: protobuf API definitions only
- `core/go/`: Go core runtime and generated protobuf packages
- `core/py/`: Python core runtime, generated protobuf packages, and uv project
- `adapters/go/`: Go adapters
- `adapters/py/`: Python adapters
- `examples/go/` and `examples/py/`: language-specific examples

This keeps the tree ready for TypeScript, Rust, and other language runtimes without mixing language implementations at the root.

## Core Model

A workflow execution is identified by:

- namespace
- workflow ID
- run ID

An activity execution is identified by:

- namespace
- workflow ID
- run ID
- activity ID

When workflow code starts or reaches an activity:

1. Read the workflow or activity record from storage, keyed by the caller-supplied id (`workflow_id+run_id` for workflows; `activity_id` under the run for activities).
2. If a completed record exists and the stored `workflow_type` /
   `activity_type` matches the current call, unmarshal and return the stored
   protobuf result.
3. If a failed record exists and the same identity checks pass, replay the stored failure rather than re-executing.
4. If no record exists, write an `IN_PROGRESS` workflow record and execute. Activities follow the retry policy on `ActivityOptions` and persist completion or terminal failure with the full attempt log.
5. If a record exists with a different `workflow_type` / `activity_type`
   (i.e. the request/response message types swapped), fail loudly instead of
   returning stale data. To intentionally execute again, use a new caller-owned
   activity or run ID, or perform the quiesced reset procedure.

Workflow records can be observed mid-flight: an `IN_PROGRESS` record is written before the body runs and stays through timer/event/dependency pending, claim contention, and activity-claim cleanup failures that interrupt the body. On other body errors, the runtime updates the record to `FAILED` and replays that failure on subsequent invocations. If invocation-claim cleanup fails after a terminal record was already persisted, the cleanup error is surfaced but the record remains terminal. Claim-capability rejection happens before the `IN_PROGRESS` write.

Activities and workflows can attach durable structured metadata via `workflow.Annotate(ctx, key, value)` (Go) or `temporaless.workflow.annotate(key, value)` (Python). Annotations are scoped to whichever record is currently being written — activity annotations land on the `ActivityRecord`, workflow-body annotations land on the `WorkflowRecord` — and survive replay because they are persisted alongside the result.

External services deliver signals with create-once `DeliverEvent` /
`SendEvent` at
`temporaless/v2/{namespace}/{workflow_id}/{run_id}/event/{event_id}.binpb`.
Identical delivery retries are idempotent; a different payload conflicts, and
a store without atomic create-if-absent support rejects the operation.
Workflow code calls `WaitEvent` (Go) or `Workflow.wait_event` (Python) and gets
either the typed payload or an `EventPendingError`. With no `PollOptions`, the
application must re-invoke the run after delivery. With caller-owned poll
options, the pending wait persists a timer that the ordinary due scanner uses
to recheck the event. This stays storage-first and requires no signal server.

The activity ID is the primary workflow authoring responsibility. It must be stable and meaningful. Reusing the same activity ID intentionally replays the stored result regardless of the new input bytes — pick a distinct id when you want a distinct execution.

Temporaless does not generate workflow IDs, run IDs, activity IDs, timer IDs, or claim owner IDs. The application owns those IDs and must pass them explicitly. Path-facing IDs are validated with Protovalidate rules declared in `temporaless.v1`.

Every workflow and activity accepts exactly one protobuf request and returns exactly one protobuf response. This mirrors ConnectRPC and gRPC handler shape in Go and keeps Python aligned with the same convention.

Shared runtime options are protobuf messages. Go and Python both use generated `WorkflowOptions` and `ActivityOptions` instead of parallel handwritten option classes.

Shared framework constants are protobuf enums. Record schema versions, timer
kinds, claim resource types/capabilities, and event-delivery
capabilities/dispositions are declared once in `temporaless.v1` and consumed
from generated Go and Python code.

`temporaless.v1.WorkflowPlan` is an optional visualization and approval
contract. It describes application-owned nodes and edges but is not interpreted
by core replay. A visual product validates and approves the plan, compiles it
to ordinary workflow code, and projects the resulting activity/timer/event
records back onto stable node IDs. This keeps graph editors optional and avoids
adding a JSON expression language or control plane to the core.

The point-storage RPC layer is defined by
`temporaless.v1.RecordStoreService`. It includes workflow, activity, timer,
create-once event delivery, claim, latest-pointer, due-ledger, and capability
calls. Local OpenDAL stores still need small language-specific infrastructure
code to render object paths and invoke each binding, but the records, keys,
statuses, capabilities, and RPC messages are generated from protobuf.

`RecordStoreService` is the cross-language core durability contract. Treat the generated protobuf request/response service shape as canonical, not the network hop. Go and Python keep small local store interfaces for workflow replay, but both can wrap a local store as an in-process service client or use generated ConnectRPC clients for remote storage. Cross-run listing, inspector search, and indexed retention live on optional `RecordQueryService` implementations. SQL and DuckLake-style stores should implement these service contracts as adapters rather than changing replay semantics. Local bucket-backed query fallbacks are for development and small deployments only; production query/search/retention should use an indexed `RecordQueryService`.

## RPC-Shaped Wrappers

Temporaless treats unary protobuf handlers as the native application shape.

In Go, a workflow or activity handler is:

```go
func(context.Context, *Request) (*Response, error)
```

In Python, a wrapped handler is `async def` end-to-end:

```python
async def fetch(request: Request) -> Response:
    ...
```

For ConnectRPC service methods (`async def m(self, req, ctx) -> Response`),
`temporaless_connectworkflow.wrap_workflow_method` wraps them as workflows
without changing the method signature; inside the body, `current_workflow()`
returns the active `Workflow` so activities, sleeps, and waits compose. Go's
equivalent is `connectworkflow.Handle(ctx, req, opts)` inside a normal
ConnectRPC handler. The adapters live under `adapters/{py,go}/connectworkflow`;
core workflow replay remains transport-agnostic.

The core provides one options-driven workflow wrapper and one options-driven activity wrapper. The options carry either fixed IDs/options or a per-request resolver. Separate fixed and dynamic wrapper variants are intentionally avoided.

Workflow IDs, run IDs, and activity IDs are still explicit. For real RPC
servers, the Python method-wrapper resolver receives the application service
and protobuf request, so IDs should come from request fields or an
application-owned routing closure. A custom transport boundary may resolve
authenticated metadata before calling the core wrapper. Temporaless must not
generate IDs or implicitly read transport headers.

## Domain Boundary

The core library owns workflow replay, activity records, timer records, claim conventions, and blessed storage infrastructure. Within core, the workflow package is the business layer. The storage package is core infrastructure and may contain the default OpenDAL implementation.

The core does not own market-data vendor clients, database writes, schedulers, queues, or trading strategy code.

Adapters should sit next to the core when they adapt an external system or compatibility target:

- ConnectRPC storage or workflow-trigger adapters
- Temporal migration adapters
- GoCDK backend-specific claim adapters
- scheduler indexes

The core should stay small enough that every function has a clear reason to exist.

## Why Protobuf Storage

All stored workflow state is protobuf binary. This gives us:

- stable schemas across Go, Python, Rust, and TypeScript clients
- deterministic serialization (records compare and replicate by bytes)
- schema evolution with Buf linting and a checked-in breaking-change policy
- native ConnectRPC request and response models

Raw JSON should not be used for framework state. Activity payloads should be protobuf messages packed into `google.protobuf.Any`.

## Why OpenDAL

OpenDAL keeps the storage surface small while still letting us run against local files, S3-compatible object stores, and cloud object stores.

Tests and examples use OpenDAL `fs` with a temporary directory. This is local, but still exercises the same durable-storage API shape as production backends.

The core depends on a tiny storage interface. OpenDAL is the default implementation, not a reason for the workflow layer to grow storage-specific branches.

## Why ConnectRPC

ConnectRPC is the transport layer for boundaries that need RPC:

- remote storage service
- activity worker service
- workflow control plane
- migration adapters

The first version can work directly with storage without a server. ConnectRPC is still in the protobuf contract so the boundary is clear when we need it.

## Application Changes

Temporaless does not pin runs to historical application builds. An
`IN_PROGRESS` run executes the handler supplied by its current invocation;
terminal workflow and activity records remain authoritative replay. Completed
activities therefore retain their stored protobuf results while previously
unrecorded work uses current code.

Applications that need two implementations to coexist use distinct
caller-owned workflow/run/activity IDs or distinct protobuf message identities.
Changing only a function or RPC method name does not change storage identity.
Build SHAs belong in annotations, logs, or traces for diagnostics, not in replay
identity.

This convention favors small, stateless deployments over an executable-artifact
catalog. Deploy workflow changes additively, preserve the meaning of existing
boundary IDs, and use a new run identity or an explicit quiesced reset for
incompatible behavior.
