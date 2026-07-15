# ConnectRPC Store Adapter

This is a decision adapter for exposing Temporaless record storage over ConnectRPC.

## Purpose

The adapter exposes the protobuf record store service so a process can read and write workflow, activity, timer, and claim records remotely.

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
- generated storage capability response for claim support

## Rejected Behavior

- no non-protobuf payloads
- no custom codecs
- no server-side workflow execution
- no production cross-run search or retention index in this adapter
- no lock service or scheduler behavior

This adapter is a transport boundary for storage records. It is not a Temporal frontend or worker service.

The runnable production-server examples are likewise storage-only. They
require explicit auth/storage configuration and do not run cron, scan timers,
or route workflow invocations. Wire background operators where the application
workflow handlers are available, or use an external scheduler/queue.
