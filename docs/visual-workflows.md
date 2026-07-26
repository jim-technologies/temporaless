# Visual Workflows

Temporaless can sit underneath an n8n-style editor without turning the core
runtime into a visual programming system. The boundary is explicit:

```text
AI or visual editor
        │ temporaless.v1.WorkflowPlan
        │ user approves deterministic plan digest
        ▼
descriptor + exact operation allowlist validation
        │
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
  // Identifies an authenticated approval record owned by the application.
  string approval_id = 4;
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

There are deliberately two validation levels:

- structural validation accepts a display-only plan and checks its graph,
  identifiers, node metadata, and exact-type data edges;
- descriptor validation is an opt-in application validation boundary; the
  runtime does not call it automatically. Every callable must use an exact
  `package.Service.Method` name, appear in the caller's explicit allowlist,
  resolve to a non-streaming protobuf RPC, and declare that method's exact
  request and response message names. Structural nodes must not carry callable
  metadata in this strict mode. Unknown protobuf fields are rejected so a
  newer producer cannot hide execution-relevant data from an older validator.

Application registry aliases such as `normalize:tweet` remain useful in a
display-only plan, but they cannot pass descriptor-aware validation. Use the
canonical protobuf RPC name in any plan that can be approved and executed. A
local handler used by an executable visual plan therefore needs a unary method
declaration in an application `.proto`; the handler registry may still invoke
it in-process, without ConnectRPC or another network transport.

The protobuf contract also caps an untrusted plan at 64 nodes, 128 edges,
64 plan annotations, and 32 annotations per node, with finite UTF-8 byte
limits on labels, descriptions, operation names, types, and annotation keys
and values. These are validation boundaries, not recommended UI sizes; visual
products should normally enforce substantially smaller product-specific
limits. The annotation key `__proto__` is rejected because JavaScript
object-backed protobuf maps cannot retain it consistently across SDKs.

Every friendly string in a plan is untrusted presentation data. Approval UIs
must render display names, descriptions, edge labels, and annotation keys and
values as escaped text—never raw HTML or unsanitized Markdown. Flag or reject
bidirectional-override/isolate characters and non-display control characters
so a label cannot visually spoof another action. Always show the canonical
`package.Service.Method`, concrete request/response types, and the relevant
typed business input independently of the friendly label; labels and
annotations never authorize execution.

Descriptor verification proves which typed RPCs the plan is allowed to name;
it cannot prove that an unrelated handwritten workflow body follows every
arrow. A visual product should compile the approved plan through one reviewed
compiler/handler registry and reuse node IDs at every durable boundary. If an
application instead maintains handwritten bodies, that plan-to-code mapping is
application code that must be tested and reviewed. Run projection keeps
unplanned durable records visible, but it is detection after execution rather
than proof before execution.

## Approval And Immutable Execution

Approval must bind to bytes, not merely to what the UI happened to display:

1. Validate the plan against the application's protobuf descriptors and exact
   operation allowlist.
2. Serialize it with deterministic protobuf serialization.
3. Compute SHA-256 and show the plan to the user.
4. After the user approves, store or sign that digest in an authenticated
   application approval system and issue an opaque `approval_id`.
5. At execution, load the approved digest by `approval_id` (or verify the
   approval system's signed token), then re-run descriptor validation and
   compare against that trusted digest immediately before entering
   `workflow.Run` / `run`.
6. Compile and execute the same private plan snapshot that was verified.
7. Ensure a changed plan cannot reuse records from an earlier plan.

The last step matters because Temporaless intentionally treats caller-owned
IDs, not input bytes, as replay identity. Applications should either:

- choose a distinct caller-supplied `run_id` for every approved plan revision
  (often including a plan-revision or digest component); or
- compare the current canonical request with the original
  `WorkflowRecord.input` before resuming an existing run and reject drift.

Temporaless does not generate the run ID or approval identity.

A digest submitted beside a plan by the same untrusted caller is not evidence
of approval: anyone can hash their own plan. The verification helpers compare
bytes; the application remains responsible for authenticating who approved
that digest and loading it from a trusted store or signature.

Plan protobuf objects are mutable in all three SDKs. Copy or canonicalize the
request into a handler-owned snapshot, verify that snapshot, and pass that
exact snapshot to the compiler. Do not verify one object and later compile a
caller-shared or reconstructed object.

Node and edge order is part of the approved protobuf value; annotation-map
ordering is canonicalized by the helpers. A builder should emit a stable order
so a cosmetic in-memory map iteration does not force reapproval.

The strict helpers are opt-in application validation primitives:

| SDK | Descriptor validation | Approval verification |
|---|---|---|
| Go | `ValidatePlanWithDescriptors` | `VerifyApprovedPlan` |
| Python | `validate_plan_with_descriptors` | `verify_approved_plan` |
| TypeScript | `validateWorkflowPlanWithDescriptors` | `verifyApprovedWorkflowPlan` |

An authoritative Go or Python server must invoke the approval function itself.
The TypeScript function is useful for a browser/Node confirmation check, but
an untrusted client cannot approve itself; repeat the check in the Go or
Python service before execution. Serve an approval view only after this trusted
boundary has validated the plan; do not use a browser-only decode of untrusted
protobuf as the authorization boundary. If a TypeScript client does receive
raw untrusted plan bytes directly, use `decodeWorkflowPlan` so non-portable map
keys are rejected before Protobuf-ES constructs its object-backed maps.

The digest covers the complete deterministic `WorkflowPlan` protobuf value.
It intentionally does not cover the descriptor set, allowlist, handler code,
business input, or run identity. Pin those through the application's canonical
request and deployment/version policy when needed.

For a high-assurance product, make the application approval record or signed
token an application-owned protobuf envelope that binds at least:

- the approved plan digest;
- a deterministic digest of the complete canonical business request;
- namespace, workflow ID, and run ID;
- a stable descriptor/policy identity for the exact RPC allowlist;
- the compiler or deployed handler release;
- approver identity, approval time, and any expiry or revocation identity.

The server authenticates that envelope, verifies every bound value against the
request and deployed policy, and only then calls the Temporaless approval
helper. Temporaless intentionally does not define this identity model because
authentication, tenant policy, release provenance, and signature management
belong to the application.

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
the plan's `operation` value. Before execution, use the strict descriptor
validator rather than trusting the three strings supplied by a planner.

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

## Declarative Systems

Temporaless should execute a declarative system, not define one universal
resource language. Resource reconciliation and workflow replay answer
different questions:

- a resource adapter owns `desired state → observed state → domain diff →
  ordered actions`;
- Temporaless durably executes those concrete protobuf actions, records their
  results, retries failures, sleeps, and waits for approvals;
- `WorkflowPlan` renders the proposed action sequence before execution and
  projects durable evidence afterward.

This is enough to host Terraform/OpenTofu-style reconciliation or ordered SQL
migrations without adding generic `Get`/`Set`/`Diff`/`Delete` RPCs to the core.
Those verbs lose important domain semantics such as replacement, rollback,
partial ordering, destructive-change policy, and migration identity. Define
the resource-specific messages and unary RPCs in the application, then adapt
Terraform, OpenTofu, Atlas, or another planner at that boundary.

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
