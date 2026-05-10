# Temporal Compatibility Adapter

This is a strict compatibility adapter for running Temporaless-shaped handlers on the real Temporal Go SDK.

It does not emulate the Temporal server. It delegates activities and durable timers to `go.temporal.io/sdk/workflow`, and the tests run against Temporal's SDK test environment.

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
