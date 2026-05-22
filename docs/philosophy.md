# Design Philosophy

This page is the short version: what Temporaless is, what it isn't, and the rules that shaped it.

## 1. A workflow is a decorated gRPC handler

There is no Temporaless-specific handler shape. You write a normal unary protobuf RPC method —

- Go: `func(ctx context.Context, req *Request) (*Response, error)`
- Python: `async def m(self, request: Request, ctx: RequestContext) -> Response`

— and the framework decorates it with replay, idempotency, and persistence:

- Python: `@wrap_workflow_method(...)` on the method.
- Go: `workflow.HandleConnect(ctx, req, WorkflowWrapOptions[...]{...})` inside the method.

Trigger surface and framework surface are the **same surface**. Anything that speaks gRPC / ConnectRPC / gRPC-Web — your existing service mesh, your existing auth, your CLI runner, an `invariantprotocol`-style adapter for HTTP/CLI/MCP — drops in unchanged.

## 2. Storage is the source of truth

There is no engine to run, no control plane to operate. Every workflow boundary (start, activity, durable timer, signal, claim) is a protobuf record at a deterministic path in object storage. On replay, the runtime reads stored records by fingerprint; on miss, it executes and writes.

Consequences:

- **Pods are interchangeable.** What you deploy is a Store backend (S3 / GCS / Azure Blob) and stateless processes calling `workflow.run`.
- **Multi-process is free.** `workflow.run` is idempotent on `(workflow_id, run_id, code_version)`. Two workers racing produce the same result.
- **Disaster recovery is the storage backup.** Records are the state.

## 3. Cross-cutting concerns ride on standard ConnectRPC interceptors

Auth, rate limiting, tracing, structured logging, tenant routing — all live in `Interceptor`s on the same `asgi_application(...)` and `ConnectStore.from_address(...)` surfaces. There is no Temporaless-specific middleware.

## 4. Async-only Python, sync Go

Both languages reject the impedance of mixing modes:

- Python uses `async def` end-to-end (storage, RPC, every adapter). Sync callables are rejected at wrap time. Reasoning: workloads are I/O-bound (LLM, vendor APIs, ML inference); modern Python is async-first; aligns with the Temporal Python SDK; lets `asyncio.gather` parallelize record I/O.
- Go uses `context.Context` and goroutines — idiomatic Go, no `async/await` machinery to invent.

`current_workflow()` (Python contextvar) and `ctx` (Go) achieve the same thing in their language's idiom: the in-flight workflow is reachable from any depth in the call stack without threading it through.

## 5. Generic, not Temporal-shaped

Temporaless is intended to be the BEST serverless workflow framework — not a Temporal clone. Options that exist in every orchestrator (retry policy, durable sleep, events, claims, annotations) belong in core. Options that only Temporal users care about (activity timeouts, heartbeats, sticky task queues, signal-channel select, workflow-level retry) belong in the `temporalcompat` adapter, where they're explicitly Temporal's idiom.

When a feature comes up: ask *would Dagster or Prefect users also want this?* If only Temporal users do, it goes in an adapter.

## 6. Stateless adapters

Inspector, janitor, timer scanner — every operations adapter is a function over the `Store` interface. The cron scheduler keeps `last_fires` in memory but exposes `Snapshot()` / `Restore()` for explicit migration, plus `LastFireFromRuns` to derive state from existing run records (zero separate persistence).

This is so you can run `N` of any adapter against the same Store without coordinating; duplicate work is idempotent.

## 7. Protobuf-first, copy over DRY

- Cross-language types (workflow options, retry policy, record schemas, statuses, claim capabilities) live in `temporaless.v1` protos. Don't add parallel handwritten Go/Python constants.
- Every function fights for existence. Three similar lines beats a premature abstraction. Abstract on the third real caller.

## 8. Carve-outs

Things this framework does *not* try to be:

- **A control plane.** No scheduler binary, no API to call. The trigger is gRPC; the state is storage.
- **A queue.** Durable timers and events are records, not queues. If you need a queue, put one in front of `workflow.run`.
- **A 1-to-1 Temporal replacement.** Use the `temporalcompat` adapter when you need Temporal semantics; use Temporal directly when you need the whole platform.
- **An ID generator.** Workflow IDs, run IDs, activity IDs, claim owner IDs are caller-provided. The framework rejects ambiguous defaults.

## 9. The shape, in one paragraph

> Take a normal protobuf RPC service. Decorate the methods you want to be durable. Mount it on any ASGI / `net/http` server, behind your existing gRPC interceptors. The framework writes one protobuf record per boundary into your Store; on replay, it reads them back. Run as many copies of your service, scheduler, scanner, and janitor as you want against the same Store — they're stateless. That's it.
