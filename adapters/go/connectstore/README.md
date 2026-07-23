# ConnectRPC Store Adapter

This is a decision adapter for exposing Temporaless record storage over ConnectRPC.

## Purpose

The adapter exposes the protobuf record store service so a process can read and
write workflow, activity, timer, event, and claim records remotely.

It also provides a local client path: `NewLocalClientStore(...)` dispatches to
the same generated service-shaped handlers in-process. Use that when the store
lives in the same process and you want the canonical protobuf RPC contract
without an HTTP hop. Use `NewHTTPClientStore(...)` when the same service is
deployed remotely.

`QueryHandler` adapts an explicit `storage.QueryStore` into
`RecordQueryService`; it never invents cross-run scans over a point store. For
small local/offline use, pass `adapters/go/scanquery`. Production inspectors,
operational search, and exact retention sweeps should pass an indexed query
adapter backed by SQL, DuckLake, or another rebuildable index.

HTTP mounting is explicit:

- `NewHTTPHandler(...)` mounts only `RecordStoreService`
- `NewHTTPHandlerWithLocalQuery(store, query, ...)` adds an explicit local
  `QueryStore`
- `NewHTTPHandlerWithQuery(...)` adds a caller-supplied indexed
  `RecordQueryService`

## Supported Behavior

- protobuf request and response messages only
- generated ConnectRPC handlers
- generated ConnectRPC clients wrapped as `storage.Store`
- in-process local clients wrapped as `storage.Store`
- explicit local or remote `storage.QueryStore` wiring
- same record keys and protobuf binary records as the core storage package
- generated storage capability response for claim and atomic event-delivery
  support
- typed `DeliverEvent` forwarding: created and idempotent dispositions plus
  structured unsupported/conflict errors

`DeliverEvent` does not make an unconditional backend atomic. The handler
advertises `CREATE_ONLY` only when its `storage.EventDeliveryStore` does, and a
client preserves that capability and the typed failure details. Direct Go
OpenDAL reports `NO_ATOMIC_CREATE` because its current binding lacks
conditional writes; use an atomic-capable remote service or a narrow native
conditional-write adapter. `PutEvent` remains the low-level replace RPC for
operators, migrations, and fixtures.

## Rejected Behavior

- no non-protobuf payloads
- no custom codecs
- no server-side workflow execution
- no production cross-run search or retention index in this adapter
- no lock service or scheduler behavior
- no check-then-write emulation for event delivery

This adapter is a transport boundary for storage records. It is not a Temporal frontend or worker service.

The runnable production-server examples are likewise storage-only. They
require explicit auth/storage configuration and do not run cron, scan timers,
or route workflow invocations. Wire background operators where the application
workflow handlers are available, or use an external scheduler/queue.
