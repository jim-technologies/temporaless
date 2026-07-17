# Temporal Adapter Contract

The core is not a Temporal clone. It is a protobuf-only replay runtime. Temporal-shaped APIs should live in adapters.

## Adapter Boundary

Temporal adapters must use strict compatibility mode.

That means an adapter wires to Temporal SDK behavior and proves compatibility with Temporal SDK tests. It must not expose familiar names while approximating Temporal behavior in Temporaless core.

The Go adapter in `adapters/go/temporalcompat` takes this position. It runs Temporaless-shaped unary protobuf handlers on the real Temporal Go SDK and tests against Temporal's SDK test environment.

The Python adapter in `adapters/py/temporalcompat` follows the same boundary. It runs Temporaless-shaped unary protobuf handlers on the real Temporal Python SDK and tests against Temporal's time-skipping test environment.

The Python `wrap_workflow` helper generates a workflow class around an existing unary protobuf function, so it disables Temporal's Python workflow sandbox for that generated class. Native sandboxed workflows should be written directly with Temporal's `@workflow.defn` and can still call this adapter's `execute_activity`, `sleep`, and wrapped activities.

Both adapters are outbound: Temporaless-shaped handlers run on the real
Temporal worker runtime. Temporal owns history and coordination in that mode.
The adapters do not execute arbitrary existing Temporal workflows on
Temporaless object storage, and they do not provide an import-only inverse
migration layer.

## Core Rules

A Temporal adapter may translate familiar usage into Temporaless conventions, but it must not weaken these rules:

- one protobuf request
- one protobuf response
- explicit workflow ID
- explicit run ID
- explicit activity/timer IDs
- explicit claim owner ID when claims are enabled
- protobuf storage only

Compatibility adapters must cover the common Temporal execution controls before they are considered useful for migration:

- activity retry policy
- start-to-close timeout
- schedule-to-close timeout
- schedule-to-start timeout when routing to a specific task queue
- heartbeat timeout when long-running activities are supported
- durable sleep/timers

If existing Temporal code uses multiple arguments, non-protobuf payloads, custom data converters, implicit serialization, child workflows, signals, queries, cancellation scopes, retry policies, or workflow task replay features that the adapter does not actually implement, the adapter must reject it.

## Required Notes

A Temporal adapter package must include a compatibility note that lists:

- Temporal SDK features supported exactly
- Temporal SDK features intentionally rejected
- Temporal SDK features approximated, if any
- test coverage against the Temporal SDK test environment
- storage assumptions and claim/lease behavior

The adapter may be narrow. Narrow is acceptable when every supported feature is truly delegated to Temporal SDK behavior and every unsupported feature is explicit.

## Useful Compatibility Tests

Do not add Temporal or Temporalite as a hard dependency of the core. If we want comparison tests, put them under an adapter test suite.

Current tests:

- wrapped unary protobuf workflow and activity values register with Temporal's
  SDK test environments
- activity execution delegates to Temporal SDK activity scheduling
- timer/sleep delegates to Temporal SDK timers
- retry policies are honored by the SDK
- timeout options are passed to the SDK and timeout failures propagate

These tests should prove adapter behavior, not drive the core into becoming a Temporal server.
