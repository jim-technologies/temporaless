# Go visualization adapter

This optional adapter gives a UI two protobuf-backed views without turning a
workflow diagram into a second execution engine:

- `WorkflowPlan` is the application-declared, approval-friendly intended graph.
- `RunProjection` joins that graph to authoritative workflow, activity, timer,
  event, and claim records after execution starts.

Plans may contain explicit loops. Removing `LOOP_BACK` edges must leave a DAG.
Callable boxes name one unary protobuf operation and its concrete request and
response message types. A `DATA` edge passes one complete protobuf response to
one compatible request; use a typed activity to transform or aggregate data.

```go
plan := &temporalessv1.WorkflowPlan{
    PlanId:   "approval:export",
    Revision: 1,
    Nodes: []*temporalessv1.WorkflowPlanNode{
        {
            NodeId:       "validate",
            DisplayName:  "Validate",
            Kind:         temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_ACTIVITY,
            Operation:    "exports.v1.ExportService.Validate",
            RequestType:  "exports.v1.ValidateRequest",
            ResponseType: "exports.v1.ValidateResponse",
        },
        {
            NodeId:      "approve",
            DisplayName: "Approve",
            Kind:        temporalessv1.WorkflowPlanNodeKind_WORKFLOW_PLAN_NODE_KIND_WAIT_EVENT,
        },
    },
    Edges: []*temporalessv1.WorkflowPlanEdge{{
        SourceNodeId: "validate",
        TargetNodeId: "approve",
        Kind:         temporalessv1.WorkflowPlanEdgeKind_WORKFLOW_PLAN_EDGE_KIND_CONTROL,
    }},
}

descriptorPolicy := visualization.DescriptorValidationOptions{
    Resolver: protoregistry.GlobalFiles,
    AllowedOperations: map[protoreflect.FullName]struct{}{
        "exports.v1.ExportService.Validate": {},
    },
}
proposalPlan := proto.Clone(plan).(*temporalessv1.WorkflowPlan)
if err := visualization.ValidatePlanWithDescriptors(
    proposalPlan,
    descriptorPolicy,
); err != nil {
    return err
}
digest, err := visualization.Digest(proposalPlan)
if err != nil {
    return err
}
// Show proposalPlan. After the user approves, the application stores this
// digest under an authenticated, opaque approval ID.
approvalID, err := approvals.Propose(ctx, proposalPlan, digest)
if err != nil {
    return err
}

// In the later execution request, load approval from the trusted application
// store. Never accept a plan and its self-computed digest as proof of approval.
executionPlan := proto.Clone(plan).(*temporalessv1.WorkflowPlan)
approvedDigest, err := approvals.ApprovedDigest(ctx, approvalID)
if err != nil {
    return err
}
if err := visualization.VerifyApprovedPlan(
    executionPlan,
    approvedDigest,
    descriptorPolicy,
); err != nil {
    return err
}
// Compile and execute executionPlan, not the caller-shared plan.

inspection, err := visualization.InspectRun(ctx, store, optionalClaimLister, runKey)
if err != nil {
    return err
}
projection, err := visualization.Project(executionPlan, inspection)
if err != nil {
    return err
}
render(projection)
```

Use each boundary node's `node_id` as its caller-supplied activity, timer, or
event ID. Activity-retry timers are joined through their recorded
`retry_activity_id`. Projection is deliberately conservative: unmatched or
wrong-kind records remain in `Unplanned*`, and the adapter never invents
“running” or “skipped” state.

`ValidatePlan` and `Digest` support display-only plans. Any plan that can reach
execution should pass `ValidatePlanWithDescriptors` or `VerifyApprovedPlan`
with an exact operation allowlist. That strict path accepts only canonical
unary protobuf RPCs, verifies exact request/response descriptors, and rejects
callable metadata on structural nodes. Import the application's generated Go
protobuf package before using `protoregistry.GlobalFiles`, or load a
self-contained descriptor set with `protodesc.NewFiles`.
