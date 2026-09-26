# Run Inspection

`temporaless.v1.RunInspectionService` is the read-only contract an operator
console, an AI agent, or a CLI uses to look at durable runs. It lives in
[`api/temporaless/v1/inspection.proto`](../api/temporaless/v1/inspection.proto),
beside the record contract, and it is deliberately separate from the two
existing services:

| Service | Writes? | Needs an index? | Role |
|---|---|---|---|
| `RecordStoreService` | yes | no | Point durability for runtimes. Its `DueTimers` may repair an interrupted ledger write. |
| `RecordQueryService` | `Sweep` deletes | yes | Cross-run search and retention over a derived index. |
| `RunInspectionService` | **never** | no | Bounded listings and point reads, plus derived history and pending state. |

Nothing in the inspection service writes, repairs, claims, or deletes, so a
server can run with read-only bucket credentials and a code bug still cannot
change a record. Read-only is a property of what is registered: a server that
projects this service over gRPC, HTTP, MCP, or a CLI exposes no mutating
method.

## Methods

| Method | Reads | Bound |
|---|---|---|
| `GetInspectionCapabilities` | configuration | none |
| `ListNamespaces` | one presence probe per configured namespace | the configured allowlist |
| `ListWorkflowDirectory` | the namespace's latest-run pointers (`_latest/`) and, for runs still in progress, their run-scoped records | page size, plus a per-request read budget |
| `ListWorkflowRuns` | the run directories of one `workflow_id` and each run's workflow record | `max_listed_runs`; beyond it the call fails and asks for an index |
| `ListScheduledWakes` | the namespace's due ledger (`_due/`), each entry's canonical timer and parent workflow record | page size, plus a per-request read budget |
| `DescribeRun` | one run's workflow, activity, timer, event, and claim records | `max_run_records` per record kind, then `truncated` |

Every request names a `store` and, apart from the capability call, a
`namespace`. An empty namespace is rejected: there is no "all namespaces"
listing, so a server can scope each caller to the namespaces it may see.

Identity always comes from record payloads. Listings order and continue by the
storage names Temporaless constructed, but a run directory without a readable
workflow record is counted (`runs_without_workflow_record`), never shown with
an identity parsed from its path. Namespaces come from configuration; the
store is probed for their presence.

Page tokens are opaque. A filtered directory or wake page may hold fewer rows
than `page_size` because each request reads a bounded number of objects and
returns a continuation token when that budget runs out.

## Derived history

Temporaless keeps point records, not an event journal. `DescribeRun` derives
`RunHistoryEvent`s from record timestamps at read time and labels them as
such. Overwritten intermediate states and released claims are not visible, and
a status the records cannot evidence (cancel, terminate, timeout,
continue-as-new) never appears.

| Record evidence | Derived events |
|---|---|
| Workflow `created_at` | `WORKFLOW_STARTED` |
| Workflow `completed_at` with COMPLETED or FAILED | `WORKFLOW_COMPLETED` or `WORKFLOW_FAILED` (with the failure) |
| Activity `attempts[i]` with `started_at` and `completed_at` | `ACTIVITY_ATTEMPT_SUCCEEDED` or `ACTIVITY_ATTEMPT_FAILED` span |
| Activity `attempts[i]` with `started_at` only | `ACTIVITY_ATTEMPT_OPEN` span |
| Activity RETRYING with `next_attempt_at` | `ACTIVITY_RETRY_BACKOFF` span from the last attempt's end |
| Activity `completed_at` with COMPLETED or FAILED | `ACTIVITY_COMPLETED` or `ACTIVITY_FAILED` |
| Timer `created_at` and `fire_at` | `TIMER_SCHEDULED` span |
| Timer `fired_at` | `TIMER_FIRED` |
| Event `received_at` | `EVENT_RECEIVED` |
| Claim `created_at` and `lease_expires_at` | `CLAIM_HELD` span (current claims only) |

A missing timestamp produces no event; the derivation never invents a time.
Events are ordered by time, then by their deterministic `event_id` (for
example `activity/fetch:page-3/attempt/2`).

## Pending state

For an IN_PROGRESS run, `RunPendingState` names the first matching reason and
the record that evidences it:

1. `OVERDUE_WAKE`: a SCHEDULED timer is past `fire_at` plus the store's
   `overdue_grace`. The timer scanner or dispatch may be stalled.
2. `RETRYING`: an activity is RETRYING (attempt, limit, last failure, and
   `next_attempt_at` when the retry is durable).
3. `STALE_CLAIM`: a claim is past its diagnostic `lease_expires_at`. The core
   never takes over claims; cleanup is a verified manual step.
4. `EXECUTING`: an unexpired workflow claim exists. A claim is evidence of an
   invocation, not proof of a healthy worker.
5. `SLEEPING`: a SCHEDULED sleep timer fires in the future.
6. `POLLING`: a SCHEDULED poll timer re-checks a condition.
7. `WAITING_NO_WAKE`: nothing durable will re-invoke the run. Alert on runs
   that stay here, per the [production checklist](production-checklist.md).

COMPLETED and FAILED runs report `TERMINAL`; a run with no workflow record
reports `UNSPECIFIED`.

## Payloads

Workflow inputs and results, activity inputs and results, and event payloads
are `google.protobuf.Any`. `DescribeRun` returns one `RenderedPayload` per
Any: ProtoJSON when the type is a well-known type or appears in a configured
descriptor set, otherwise the opaque bytes. When the caller may not see
payloads, the response reports `PAYLOAD_VISIBILITY_REDACTED`: each Any keeps
its type URL with an empty value and the rendered payload carries only the
type and original size.

## Implementations

[`adapters/go/inspection`](../adapters/go/inspection) serves the contract
directly over OpenDAL bucket stores, with per-caller store, namespace, and
payload scoping and bounded listings. It is the backend of the optional
[read-only console](console.md). Index-backed global search (filter every run by status,
type, or time) is a later `RecordQueryService` extension; bucket-native
inspection reports `indexed_search=false`.

## Generated code

| Language | Location |
|---|---|
| Go | `core/go/gen/temporaless/v1/inspectionv1` (messages and gRPC stubs) and `inspectionv1/inspectionv1connect` |
| Python | `temporaless.v1.inspection_pb2`, `temporaless.v1.inspection_connect` |
| TypeScript | `@jim-technologies/temporaless/gen/temporaless/v1/inspection` |

The Go messages generate into their own package, not `temporalessv1`, so the
core record package never links a gRPC transport. That is the one
`PACKAGE_SAME_GO_PACKAGE` exception in `buf.yaml`. Only this service gets gRPC
stubs, which Invariant Protocol needs to register it.
