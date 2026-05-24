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

disp := dispatch.New(dispatch.Options{
    DrainTimeout: 15 * time.Second,
})

// Register handlers under their gRPC fully-qualified method names so the
// same identity used at the wire layer routes here too.
dispatch.Register(disp, "/payments.Charges/Charge", server.Charge)
dispatch.Register(disp, "/payments.Charges/Refund", server.Refund)

// Fire-and-forget — returns immediately:
_ = disp.DoAsync(ctx, "/payments.Charges/Charge", &ChargeRequest{Amount: 100})

// SIGTERM handler:
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
defer cancel()
_ = disp.Shutdown(shutdownCtx)
```

Handler errors flow through `Options.OnError` (default: WARN-level
`slog.Default()`). Override to route into your telemetry pipeline.

Panicking handlers are recovered and surfaced through the same path so a
single bad call can't crash the process.
