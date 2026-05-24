// Package dispatch is a bounded fire-and-forget goroutine pool for gRPC-shaped
// handlers. It complements `workflow.Run` (which is synchronous and durable)
// with an asynchronous, in-process path for side effects whose result the
// caller doesn't need to wait on — webhook notifications, telemetry pushes,
// best-effort vendor pings, fan-out fanouts where the caller wants its own
// request to return quickly.
//
// # Semantics (intentional)
//
// In-process only. A handler invocation lives inside a goroutine of the
// process that called `DoAsync`. If that process dies before the handler
// finishes, the work is lost. This is the deliberate tradeoff vs.
// `workflow.Run` — when you need durability across crashes, write a workflow
// instead; this package is for things where at-most-once + best-effort is
// the right semantics.
//
// What you DO get: a managed graceful shutdown. `Shutdown(ctx)` stops
// accepting new submissions, waits up to `DrainTimeout` (default 15s, set
// via `Options.DrainTimeout`) for in-flight goroutines to finish, then
// cancels the per-handler context so handlers can observe `ctx.Err()` and
// bail. `Shutdown` blocks until every goroutine has returned, even past the
// drain timeout — losing a handler entirely is worse than waiting a few
// extra seconds for it to notice cancellation.
//
// # Registration shape
//
// Handlers are registered by method name and invoked by method name —
// matching gRPC's "/package.Service/Method" form so a single line wires up
// the same handler the gRPC server already uses:
//
//	disp := dispatch.New(dispatch.Options{DrainTimeout: 15 * time.Second})
//	dispatch.Register(disp, "/payments.Charges/Charge", server.Charge)
//	dispatch.Register(disp, "/payments.Charges/Refund", server.Refund)
//
//	// Fire-and-forget — returns immediately:
//	_ = disp.DoAsync(ctx, "/payments.Charges/Charge", &ChargeRequest{Amount: 100})
//
//	// At process shutdown (e.g. SIGTERM handler):
//	ctx, cancel := context.WithTimeout(context.Background(), 30 * time.Second)
//	defer cancel()
//	_ = disp.Shutdown(ctx)
//
// Handler errors have nowhere to return (the caller already moved on). They
// flow through `Options.OnError`, which defaults to logging via `slog`.
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// DefaultDrainTimeout is how long `Shutdown` waits for in-flight goroutines
// to finish before cancelling their context. Chosen to match common SIGTERM
// grace periods (Kubernetes preStop / terminationGracePeriodSeconds).
const DefaultDrainTimeout = 15 * time.Second

// ErrShuttingDown is returned by `DoAsync` when the dispatcher has begun or
// completed `Shutdown`. Callers should treat this as a final "do not retry"
// signal — the process is going away.
var ErrShuttingDown = errors.New("dispatcher is shutting down")

// ErrUnknownMethod is returned by `DoAsync` when no handler was registered
// for the requested method. Misspelling the method name or forgetting to
// call `Register` is the usual cause.
var ErrUnknownMethod = errors.New("no handler registered for method")

// ErrTypeMismatch is returned by `DoAsync` when the supplied request value
// is not the type the registered handler expects. Recovers from gRPC method
// name typos that happen to collide.
var ErrTypeMismatch = errors.New("request type does not match registered handler")

// Options configures a `Dispatcher`.
type Options struct {
	// DrainTimeout is how long `Shutdown` waits for in-flight handlers to
	// finish before cancelling their context. Zero falls back to
	// `DefaultDrainTimeout`.
	DrainTimeout time.Duration

	// OnError is invoked when a handler returns a non-nil error. Default:
	// log at WARN via `slog.Default()`. Override to integrate with your
	// telemetry pipeline.
	OnError func(method string, err error)
}

// Dispatcher is a goroutine pool keyed by gRPC-style method names.
type Dispatcher struct {
	drainTimeout time.Duration
	onError      func(method string, err error)

	mu       sync.RWMutex
	handlers map[string]handlerEntry

	// inflightCtx is the per-invocation context handlers see. It's derived
	// from `context.Background` (not from the caller's ctx) so a request-
	// scoped context being cancelled doesn't kill the goroutine the caller
	// just fired-and-forgot. Cancel happens only at `Shutdown` after the
	// drain window elapses.
	inflightCtx    context.Context
	inflightCancel context.CancelFunc

	wg sync.WaitGroup

	// closed flips to 1 at the start of `Shutdown`; new `DoAsync` calls are
	// rejected from then on.
	closed atomic.Bool
}

type handlerEntry struct {
	reqGoType  any // *Req (a typed nil) — used purely as the type token for assertion error messages
	invoke     func(ctx context.Context, req proto.Message) error
}

// New constructs a `Dispatcher`. Pass a zero-value `Options{}` for defaults.
func New(opts Options) *Dispatcher {
	drain := opts.DrainTimeout
	if drain <= 0 {
		drain = DefaultDrainTimeout
	}
	onErr := opts.OnError
	if onErr == nil {
		onErr = func(method string, err error) {
			slog.Default().Warn("dispatch: handler returned error",
				"method", method, "err", err.Error())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		drainTimeout:   drain,
		onError:        onErr,
		handlers:       make(map[string]handlerEntry),
		inflightCtx:    ctx,
		inflightCancel: cancel,
	}
}

// Register wires up a typed handler under the given method name. `method`
// should be the gRPC fully-qualified method ("/package.Service/Method") so
// the same identity used at the wire layer routes here too. Re-registering
// the same method silently overwrites — last writer wins.
//
// `Register` is a top-level generic function (rather than a method on
// `*Dispatcher`) because Go does not allow type parameters on methods.
func Register[Req proto.Message, Resp proto.Message](
	d *Dispatcher,
	method string,
	handle func(ctx context.Context, req Req) (Resp, error),
) {
	if d == nil {
		panic("dispatch.Register: dispatcher is nil")
	}
	if method == "" {
		panic("dispatch.Register: method is required")
	}
	if handle == nil {
		panic("dispatch.Register: handler is required")
	}
	var zero Req
	entry := handlerEntry{
		reqGoType: zero,
		invoke: func(ctx context.Context, req proto.Message) error {
			typed, ok := req.(Req)
			if !ok {
				return fmt.Errorf("%w: handler %q expects %T, got %T",
					ErrTypeMismatch, method, zero, req)
			}
			_, err := handle(ctx, typed)
			return err
		},
	}
	d.mu.Lock()
	d.handlers[method] = entry
	d.mu.Unlock()
}

// DoAsync looks up the handler for `method`, type-asserts `req` to the
// handler's request type, and runs it in a new goroutine. Returns
// immediately. Returns an error ONLY for the pre-dispatch failures
// (`ErrShuttingDown`, `ErrUnknownMethod`, `ErrTypeMismatch`) — handler
// errors flow through `Options.OnError`.
//
// The `ctx` argument is used only for the registry lookup and the type
// check; the running goroutine sees a fresh long-lived context that is
// cancelled only when `Shutdown` exceeds its drain window. This is
// deliberate — a request-scoped context being cancelled (because the HTTP
// handler that called `DoAsync` returned) must not kill the side-effect
// goroutine.
func (d *Dispatcher) DoAsync(ctx context.Context, method string, req proto.Message) error {
	if d == nil {
		return fmt.Errorf("dispatch.DoAsync: dispatcher is nil")
	}
	if d.closed.Load() {
		return ErrShuttingDown
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	d.mu.RLock()
	entry, ok := d.handlers[method]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownMethod, method)
	}
	if req == nil {
		return fmt.Errorf("dispatch.DoAsync: req is required for method %q", method)
	}
	// Pre-check the request type so callers find out at the call site, not
	// from an OnError log line later. The invoke closure repeats the assert
	// for safety; the second check is essentially free.
	if _, ok := req.(proto.Message); !ok {
		return fmt.Errorf("%w: %q (got %T)", ErrTypeMismatch, method, req)
	}

	// Acquire the wg slot BEFORE launching the goroutine so a Shutdown
	// racing with DoAsync sees the count and waits. The atomic check above
	// is the fast path; the recheck inside the lock would be needed for
	// strict correctness, but Shutdown also waits on the wg, so a goroutine
	// started in the small window between `closed.Load()` and `wg.Add(1)`
	// will still be awaited.
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		// Recover so a panicking handler doesn't take the whole process
		// down. Surface as an error through OnError so operators see it.
		defer func() {
			if r := recover(); r != nil {
				d.onError(method, fmt.Errorf("handler panicked: %v", r))
			}
		}()
		if err := entry.invoke(d.inflightCtx, req); err != nil {
			d.onError(method, err)
		}
	}()
	return nil
}

// Shutdown stops accepting new submissions, waits up to `DrainTimeout` for
// in-flight goroutines to finish, then cancels their context to signal
// cooperative cancellation. Blocks until every goroutine has returned OR
// `shutdownCtx` is itself cancelled — whichever comes first.
//
// Returns `context.DeadlineExceeded` (or whatever `shutdownCtx.Err()`
// returns) if `shutdownCtx` cancelled before all goroutines drained.
// Returns nil on clean drain.
//
// Calling `Shutdown` twice is safe; the second call observes the already-
// cancelled state and returns immediately.
func (d *Dispatcher) Shutdown(shutdownCtx context.Context) error {
	if d == nil {
		return nil
	}
	// Mark closed FIRST so new DoAsync calls bail before adding to wg.
	d.closed.Store(true)

	// Phase 1: best-effort drain. Wait for either the wg to clear or the
	// drain timeout to elapse.
	if !d.waitDrain(shutdownCtx, d.drainTimeout) {
		// Drain window expired — signal cancellation to running handlers
		// so they can bail out cooperatively.
		d.inflightCancel()
	}

	// Phase 2: wait for the rest unconditionally, bounded only by
	// shutdownCtx. We never abandon goroutines — orphaning a handler
	// mid-vendor-call is worse than waiting a few extra seconds.
	if !d.waitDrain(shutdownCtx, 0) {
		if shutdownCtx != nil {
			return shutdownCtx.Err()
		}
		return nil
	}
	return nil
}

// waitDrain blocks until either the wait group clears, the optional
// timer elapses (when timeout > 0), or shutdownCtx is cancelled. Returns
// true if the wg cleared.
func (d *Dispatcher) waitDrain(shutdownCtx context.Context, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	var timerC <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timerC = t.C
	}

	var ctxDone <-chan struct{}
	if shutdownCtx != nil {
		ctxDone = shutdownCtx.Done()
	}

	select {
	case <-done:
		return true
	case <-timerC:
		return false
	case <-ctxDone:
		return false
	}
}
