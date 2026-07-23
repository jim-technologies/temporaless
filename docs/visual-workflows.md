# Visual Workflows

Temporaless can sit underneath an n8n-style editor without turning the core
runtime into a visual programming system. The boundary is explicit:

```text
AI or visual editor
        │ temporaless.v1.WorkflowPlan
        │ user approves deterministic plan digest
        ▼
application compiler / typed handler registry
        │ ordinary Go or Python workflow body
        ▼
Temporaless activities, timers, events, dependencies, and records
        │
        ▼
plan + run snapshot → live UI
```

The plan is authoritative for intended topology. Stored records are
authoritative for what actually executed. Temporaless never attempts to infer
an authoritative graph from arbitrary Go or Python source.

## Plan Contract

`temporaless.v1.WorkflowPlan` is optional display and approval metadata. It
contains stable nodes and labelled edges, but it is deliberately not a second
execution language:

- callable nodes identify one unary protobuf operation and its concrete
  request/response message types;
- activity, timer, and event node IDs should be used unchanged as the
  corresponding Temporaless boundary IDs;
- control, exact-type data, and conditional edges describe forward flow;
- conditional edges originate at a branch node and use one unique label per
  possible route;
- loops use an explicit `LOOP` node and `LOOP_BACK` edge, while each bounded
  iteration still receives a caller-supplied stable activity ID;
- annotations are for display and filtering, never execution decisions.

An application normally embeds the plan in its own canonical request:

```proto
message ExecuteExportRequest {
  string workflow_id = 1;
  string run_id = 2;
  temporaless.v1.WorkflowPlan plan = 3;
  string approved_plan_sha256_hex = 4;
  ExportInput input = 5;
}

message ExecuteExportResponse {
  ExportResult result = 1;
}

service ExportService {
  rpc ExecuteExport(ExecuteExportRequest) returns (ExecuteExportResponse);
}
```

The business input remains concrete protobuf. Do not replace it with JSON,
`Struct`, arbitrary argument bags, or a custom expression codec.

Go exposes the optional adapter under `adapters/go/visualization`. Python
exposes the equivalent helpers as `temporaless.visualization`, and TypeScript
exports the same validation, digest, inspection, and projection concepts from
the root Git package.

## Approval And Immutable Execution

Approval must bind to bytes, not merely to what the UI happened to display:

1. Validate the plan.
2. Serialize it with deterministic protobuf serialization.
3. Compute SHA-256 and show the plan to the user.
4. Store or sign that digest in the application approval system.
5. Verify the approved digest before entering `workflow.Run` / `run`.
6. Ensure a changed plan cannot reuse records from an earlier plan.

The last step matters because Temporaless intentionally treats caller-owned
IDs, not input bytes, as replay identity. Applications should either:

- choose a distinct caller-supplied `run_id` for every approved plan revision
  (often including a plan-revision or digest component); or
- compare the current canonical request with the original
  `WorkflowRecord.input` before resuming an existing run and reject drift.

Temporaless does not generate the run ID or approval identity.

Node and edge order is part of the approved protobuf value; annotation-map
ordering is canonicalized by the helpers. A builder should emit a stable order
so a cosmetic in-memory map iteration does not force reapproval.

## Common Visual Nodes

| Visual box | Temporaless compilation |
|---|---|
| Local function | `ExecuteActivity` / `Workflow.execute_activity` around the unary protobuf handler |
| Remote ConnectRPC method | Generated client call inside an activity; network I/O never runs directly in replay logic |
| Sequence | Ordinary call/`await` order |
| Branch | A recorded decision activity returning a protobuf enum, followed by ordinary `if`/`switch` |
| Fan-out / fan-in | `AllActivities` / `gather_activities`, which settles every started branch |
| Bounded foreach / loop | Ordinary loop with every iteration ID present in the approved plan |
| Durable delay | `Sleep` / `Workflow.sleep`; use the plan node ID as `timer_id` |
| Approval or webhook | `WaitEvent` / `Workflow.wait_event`; use the plan node ID as `event_id` and, when polling, as `PollOptions.timer_id` |
| Upstream workflow | `dependencies.WaitForWorkflow` / `wait_for_workflow` with explicit workflow and run IDs; when polling, use the plan node ID as `PollOptions.timer_id` |

An upstream-workflow box is a dependency, not a first-class child workflow.
Temporaless does not currently claim parent/child cancellation, lineage, or
history semantics. If a visual product wants a “subflow” box, it must
idempotently trigger the canonical child RPC and then wait for that explicit
run; the product owns the relationship.

## Typed Edges

A builder may register local handlers or generated ConnectRPC methods under
the plan's `operation` value. Before execution it should compare the
registered input/output descriptors with `request_type` and `response_type`.

A `DATA` edge passes an entire protobuf response to a node that accepts that
exact message type; validators reject descriptor mismatches and more than one
incoming data edge. A `CONTROL` edge expresses ordering without claiming to
map payload fields. When data shapes differ or fan-in must construct a new
request, insert another typed transformation/aggregation activity. Temporaless
deliberately does not define a JSON-path, template, or field-expression
language.

Branches follow the same rule. The branch node should be an activity whose
protobuf response contains a stable enum or route identifier. That result is
durable, so replay cannot choose another path because wall-clock time or an
external API changed.

## Plan Versus Actual

The visualization helpers read one run snapshot:

- `WorkflowRecord`;
- activity records;
- timer records;
- delivered event records;
- coordination claims.

Projection matches a plan node ID to the same activity, timer, event, or claim
resource ID. It also returns unplanned records so a UI can flag code that
executed outside the approved plan.

The adapter intentionally returns evidence rather than inventing a perfect
state:

- completed, failed, and retrying activity records are exact;
- scheduled/fired timers and delivered events are exact;
- a matching claim is evidence that a boundary is claimed, but it is not proof
  that a worker is healthy;
- an absent event record cannot distinguish “not reached” from “currently
  waiting” without the plan and completed dependencies;
- structural fan-out and loop nodes do not have their own durable record unless
  the application models them as activities.

A UI may derive friendly labels such as *planned*, *waiting*, or *claimed*,
but should retain the underlying evidence and avoid claiming source-inferred
causality.

## Existing Runnable Coverage

- `examples/py/data_pipeline.py` covers sequence, typed activities,
  conditional branching, fan-out/fan-in, checkpoints, and backfill.
- `examples/py/approval_workflow.py` covers durable sleep, approval events,
  process exit, resumption, and replay.
- `examples/go/quant-service` covers canonical ConnectRPC workflow methods and
  all-settled fan-out.
- `examples/go/twitter-webhook` covers an event-driven branch and replay.
- `examples/{go,py}` scheduling examples cover cron-driven run creation.

These examples are compiler targets for a visual product: a builder produces
the confirmed plan and equivalent workflow body, while the existing
Temporaless primitives provide execution durability.
