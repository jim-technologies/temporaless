# Temporal Compatibility Adapter

This is a strict compatibility adapter for running Temporaless-shaped handlers on the real Temporal Go SDK.

It does not emulate the Temporal server. It delegates activities and durable timers to `go.temporal.io/sdk/workflow`, and the tests run against Temporal's SDK test environment.

The wrapper values are registered with the normal Temporal worker API. Use
explicit Temporal registration names because Go closures returned by the
generic wrappers do not provide useful application names:

```go
wrappedActivity := temporalcompat.WrapActivity(
    temporalcompat.ActivityWrapOptions[*FetchRequest, *FetchResponse]{
        Execute: fetchPrice,
    },
)
wrappedWorkflow := temporalcompat.WrapWorkflow(
    temporalcompat.WorkflowWrapOptions[*PriceRequest, *PriceResponse]{
        Execute: priceWorkflow,
    },
)

worker.RegisterActivityWithOptions(
    wrappedActivity,
    activity.RegisterOptions{Name: "FetchPrice"},
)
worker.RegisterWorkflowWithOptions(
    wrappedWorkflow,
    workflow.RegisterOptions{Name: "PriceWorkflow"},
)
```

An ordinary unary protobuf activity body can remain unchanged. A workflow body
still uses `workflow.Context` and Temporal activity/timer APIs, so a native
Temporal workflow is not converted into a Temporaless workflow by changing an
import.

## Supported

- one protobuf workflow request and one protobuf workflow response
- one protobuf activity request and one protobuf activity response
- Temporal SDK activity scheduling through `workflow.ExecuteActivity`
- Temporal SDK durable timers through `workflow.Sleep`
- Temporal SDK `workflow.ActivityOptions`

## Rejected

- multiple workflow or activity arguments
- non-protobuf payloads
- custom Temporal payload converter behavior hidden behind Temporaless APIs
- child workflows, signals, queries, updates, cancellation scopes, and side effects

Those features should use the Temporal SDK directly until the adapter can prove exact compatibility for them.

## Compatibility Position

This adapter is compatible by wiring to the Temporal SDK rather than approximating Temporal semantics in Temporaless core. It is intentionally narrow: it lets a Temporaless unary protobuf handler shape run inside a Temporal worker, and it keeps Temporal-specific behavior out of the core runtime.

Temporal owns history and worker coordination in this mode. Temporaless object
storage, claims, and replay records are not involved unless application code
separately invokes a Temporaless workflow boundary.
