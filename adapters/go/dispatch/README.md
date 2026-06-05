# dispatch

Bounded fire-and-forget goroutine pool for gRPC-shaped handlers.

Complements `workflow.Run` (synchronous + durable) with an asynchronous,
in-process path for side effects whose result the caller doesn't need to
wait on — webhook notifications, telemetry pushes, best-effort vendor
pings, fan-out where the caller wants its own request to return quickly.

**Not durable.** If the process dies before a handler finishes, the work
is lost. When you need durability across crashes, write a workflow
instead — this package is for at-most-once + best-effort.

**Managed graceful shutdown.** `Shutdown(ctx)` stops accepting new
submissions, waits up to `DrainTimeout` (default 15s) for in-flight
goroutines to finish, then cancels the per-handler context. Always waits
for every goroutine to return — orphaning a handler mid-vendor-call is
worse than waiting a few extra seconds for it to notice cancellation.

```go
import "github.com/jim-technologies/temporaless/adapters/go/dispatch"

// Options are proto-declared so they round-trip across SDKs from a single
// config file / env / CLI flag.
disp := dispatch.New(dispatch.Options{
    Proto: &temporalessv1.DispatchOptions{
        DrainTimeout: durationpb.New(15 * time.Second),
        MaxInflight:  100, // optional cap; DoAsync blocks above the cap
    },
    // Queue: nil // default in-process pool; swap for Kafka/Rabbit/SQS/...
})

// Register handlers under their gRPC fully-qualified method names so the
// same identity used at the wire layer routes here too.
dispatch.Register(disp, "/payments.Charges/Charge", server.Charge)
dispatch.Register(disp, "/payments.Charges/Refund", server.Refund)

// DoAsync returns the per-submission task_id immediately:
taskID, err := disp.DoAsync(ctx, "/payments.Charges/Charge", &ChargeRequest{Amount: 100})

// Poll for completion. Status walks PENDING → RUNNING → DONE/FAILED.
// On DONE, info.Response carries the handler's typed response wrapped
// in google.protobuf.Any:
info, ok := disp.Status(taskID)
if ok && info.Status == temporalessv1.TaskStatus_TASK_STATUS_DONE {
    out := &ChargeResponse{}
    _ = info.Response.UnmarshalTo(out)
}

// SIGTERM handler:
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
defer cancel()
_ = disp.Shutdown(shutdownCtx)
```

Handler errors flow through `Options.OnError` (default: WARN-level
`slog.Default()` with method + task_id). The same error is also recorded
on the TaskInfo (`info.Error`), so polling clients see it.

Panicking handlers are recovered and surfaced through the same path so a
single bad call can't crash the process.

## Task tracking (`Status`)

Every `DoAsync` submission gets a ULID task_id and an in-memory
`TaskInfo` record. The record walks PENDING → RUNNING → DONE or FAILED;
completed records stay queryable until `Proto.TaskTtl` (default 1 hour)
elapses, then evict. In-flight records never evict — losing one
mid-flight is the worst possible failure mode for an observability tool,
so the framework refuses to do it.

Tracking is always on. The cost is one map entry per submission; the
opinion is that "I want to know if my background work succeeded" is
table stakes for every consumer worth shipping. If you truly want
fire-and-forget with no record (webhook notifications etc.), just
ignore the returned task_id; the TTL sweep takes care of the rest.

```go
taskID, err := disp.DoAsync(ctx, "/billing/Charge", req)
// ... time passes ...
info, ok := disp.Status(taskID)
if !ok { /* unknown id — never existed, or TTL evicted */ }
switch info.Status {
case TASK_STATUS_PENDING, TASK_STATUS_RUNNING:
    // keep polling
case TASK_STATUS_DONE:
    resp := &ChargeResponse{}
    info.Response.UnmarshalTo(resp)
case TASK_STATUS_FAILED:
    log.Warn("charge failed", "task_id", info.TaskId, "err", info.Error)
}
```

## Backpressure (`MaxInflight`)

By default a `Dispatcher` is unbounded — one goroutine per `DoAsync`
call. For a server that needs to cap concurrent vendor calls or bound
memory under burst load, set `Proto.MaxInflight`. `DoAsync` then
blocks until a slot is available, respecting:

- the caller's `ctx` (returns `ctx.Err()` if it cancels first)
- the dispatcher's shutdown signal (returns `ErrShuttingDown` so a
  SIGTERM doesn't strand callers waiting for slots that will never come)
- a slot becoming free (proceeds normally)

This gives you natural per-process backpressure without a queue: bursty
callers get throttled at the submit point.

## External queues (Kafka, RabbitMQ, NATS, SQS, ...)

Pass `Options.Queue` with any type that implements the `Queue` interface:

```go
type Queue interface {
    Submit(ctx context.Context, method, taskID string, payload []byte) error
    Close(ctx context.Context) error
}
```

The dispatcher proto-marshals the request to deterministic bytes and
hands `(method, taskID, payload)` to your queue. The task_id is the
ULID the producer side returned to its caller; external queue
implementations should propagate it (message header, attribute, custom
field) so consumers can correlate. Your consumer process pulls messages
off the bus and calls `dispatcher.Invoke(ctx, method, payload)`
— which looks up the registered handler, unmarshals the bytes back into
the typed `Req`, and runs the handler synchronously on the consumer
goroutine. Use the returned error to drive your queue's ack / nack.

The framework intentionally doesn't ship Kafka / Rabbit adapters — each
has its own connection management, consumer-group semantics, and
prefetch tuning. Writing the adapter is ~50 LOC of the `Queue`
interface + your usual client setup.
