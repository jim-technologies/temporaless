# ConnectRPC Store Adapter

This is a decision adapter for exposing Temporaless record storage over ConnectRPC.

## Purpose

The adapter exposes the protobuf record store service so a process can read and write workflow, activity, timer, and claim records remotely.

It also provides a local client path: `NewLocalClientStore(...)` dispatches to
the same generated service-shaped handlers in-process. Use that when the store
lives in the same process and you want the canonical protobuf RPC contract
without an HTTP hop. Use `NewHTTPClientStore(...)` when the same service is
deployed remotely.

The bundled `QueryHandler` is a local/development fallback only. It adapts the
plain `storage.Store` surface into `RecordQueryService`, rejects ordering and
pagination, and may walk workflow runs for broad activity listing. Production
inspectors, operational search, and exact retention sweeps should use an
indexed `RecordQueryService` implementation backed by SQL, DuckLake, or another
rebuildable query index.

HTTP mounting is explicit:

- `NewHTTPHandler(...)` mounts only `RecordStoreService`
- `NewHTTPHandlerWithLocalQuery(...)` adds the local/development query fallback
- `NewHTTPHandlerWithQuery(...)` adds a caller-supplied indexed
  `RecordQueryService`

## Supported Behavior

- protobuf request and response messages only
- generated ConnectRPC handlers
- generated ConnectRPC clients wrapped as `storage.Store`
- in-process local clients wrapped as `storage.Store`
- local/development query fallback for small stores
- same record keys and protobuf binary records as the core storage package
- generated storage capability response for claim support

## Rejected Behavior

- no non-protobuf payloads
- no custom codecs
- no server-side workflow execution
- no production cross-run search or retention index in this adapter
- no lock service or scheduler behavior

This adapter is a transport boundary for storage records. It is not a Temporal frontend or worker service.
