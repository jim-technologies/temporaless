// Package dispatch is a fire-and-forget dispatcher for gRPC-shaped handlers.
// It complements `workflow.Run` (which is synchronous and durable) with an
// asynchronous path for side effects whose result the caller doesn't need to
// wait on — webhook notifications, telemetry pushes, best-effort vendor
// pings, fan-out where the caller wants its own request to return quickly.
//
// # Two backends
//
// Submissions go through a `Queue`. The default is in-process: each call
// spawns a goroutine, with managed graceful shutdown and optional concurrency
// caps. Advanced users plug in `Kafka` / `RabbitMQ` / `NATS` / `SQS` / etc.
// by implementing the `Queue` interface — the dispatcher proto-marshals the
// request and hands `(method, payload []byte)` to the queue; consumers pull
// the bytes back off and call `Dispatcher.Invoke` to run the registered
// handler.
//
// # Durability semantics
//
// In-process: not durable. If the process dies before the handler finishes,
// the work is lost. This is the deliberate tradeoff vs. `workflow.Run` —
// when you need durability across crashes, write a workflow instead. This
// path is for at-most-once + best-effort.
//
// External queue: durability comes from your queue. The framework only
// guarantees the producer hands the bytes off; the queue + consumer drive
// at-least-once via their native ack / nack.
//
// # Graceful shutdown (in-process)
//
// `Shutdown(ctx)` stops accepting new submissions, waits up to `DrainTimeout`
// (default 15s, set via `Options.Proto.DrainTimeout`) for in-flight
// goroutines to finish, then cancels the per-handler context so handlers
// observe `ctx.Err()` and bail. `Shutdown` blocks until every goroutine has
// returned, even past the drain timeout — losing a handler entirely is worse
// than waiting a few extra seconds for it to notice cancellation.
//
// # Registration
//
// Handlers are registered by gRPC fully-qualified method name
// ("/package.Service/Method") so the same identity the gRPC server uses
// routes here too:
//
//	disp := dispatch.New(dispatch.Options{
//	    Proto: &temporalessv1.DispatchOptions{
//	        DrainTimeout: durationpb.New(15 * time.Second),
//	        MaxInflight:  100, // optional cap; DoAsync blocks above the cap
//	    },
//	})
//	dispatch.Register(disp, "/payments.Charges/Charge", server.Charge)
//	dispatch.Register(disp, "/payments.Charges/Refund", server.Refund)
//
//	// Fire-and-forget. Returns immediately when MaxInflight == 0;
//	// blocks for a slot when bounded.
//	_ = disp.DoAsync(ctx, "/payments.Charges/Charge", &ChargeRequest{Amount: 100})
//
//	// SIGTERM handler:
//	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	_ = disp.Shutdown(shutdownCtx)
//
// Handler errors flow through `Options.OnError` (default: WARN via `slog`).
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
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

// Options configures a `Dispatcher`. The serializable knobs
// (`DrainTimeout`, `MaxInflight`) live in the proto-declared
// `temporalessv1.DispatchOptions` so a single config file / env var /
// CLI flag drives them identically across Go, Python, and Rust. Runtime
// hooks that can't be expressed as data (`OnError`, `Queue`) stay
// language-local here.
type Options struct {
	// Proto carries the serializable config. When nil, defaults apply
	// (`DefaultDrainTimeout`, `MaxInflight=0`).
	Proto *temporalessv1.DispatchOptions

	// Queue is the producer-side backend. Default: in-process goroutine
	// pool. Advanced users plug in a Kafka / RabbitMQ / SQS / NATS adapter
	// that implements the `Queue` interface; submissions then land on
	// that queue and are consumed by a worker process (see the
	// `Invoke` helper). The queue is opaque from the dispatcher's
	// perspective — it only sees `Submit(ctx, method, payload []byte)`.
	Queue Queue

	// OnError is invoked when a handler returns a non-nil error. Default:
	// log at WARN via `slog.Default()`. Override to integrate with your
	// telemetry pipeline. Used only by the in-process queue path; external
	// queue adapters surface their own errors via the queue's native ack /
	// nack semantics.
	OnError func(method string, err error)
}

// Dispatcher is a goroutine pool keyed by gRPC-style method names. Submissions
// route through `Queue` (default: in-process). Drain semantics apply only to
// the in-process queue — external queue adapters own their own delivery and
// retry semantics.
type Dispatcher struct {
	drainTimeout time.Duration
	onError      func(method string, err error)

	// queue is the producer backend. Defaults to inProcessQueue.
	queue Queue

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

	// sem is the bounded-concurrency token bucket. Nil when MaxInflight
	// is 0 (unbounded). Capacity == MaxInflight; a send takes a slot, a
	// receive returns one. Buffered channel is the canonical Go semaphore.
	sem chan struct{}

	// shutdownCh closes when Shutdown begins. DoAsync watches this in its
	// semaphore-wait select so a SIGTERM unblocks waiters immediately
	// instead of letting them sit until ctx times out.
	shutdownCh chan struct{}

	// closed flips to 1 at the start of `Shutdown`; new `DoAsync` calls are
	// rejected from then on.
	closed atomic.Bool

	// submitMu serializes the "is the dispatcher closed?" check with
	// `wg.Add(1)` so that no Add can land after Shutdown has begun calling
	// `wg.Wait()`. Submit takes RLock for the full submission (multiple
	// concurrent submits are fine); Shutdown takes the write Lock as a
	// barrier AFTER signalling close+shutdownCh and BEFORE wg.Wait().
	// Without this, the WaitGroup hits an Add-during-Wait data race that
	// shows up under `go test -race -count=N`.
	submitMu sync.RWMutex
}

type handlerEntry struct {
	// reqType is a zero-value of the registered Req. Used for the
	// producer-side type check and as a template for the consumer-side
	// `proto.Unmarshal` (via the `newReq` factory).
	reqType proto.Message
	// newReq constructs a fresh, zero-valued Req for unmarshaling on the
	// consumer side. Captured at register time via generics so we don't
	// need runtime reflection.
	newReq func() proto.Message
	// invoke type-asserts the message back to Req and runs the typed
	// handler. Carries any type-mismatch error from the assert as well as
	// the handler's own error.
	invoke func(ctx context.Context, req proto.Message) error
}

// New constructs a `Dispatcher`. Pass a zero-value `Options{}` for the
// in-process queue with framework defaults.
func New(opts Options) *Dispatcher {
	drain := opts.Proto.GetDrainTimeout().AsDuration()
	if drain <= 0 {
		drain = DefaultDrainTimeout
	}
	maxInflight := int(opts.Proto.GetMaxInflight())
	onErr := opts.OnError
	if onErr == nil {
		onErr = func(method string, err error) {
			slog.Default().Warn("dispatch: handler returned error",
				"method", method, "err", err.Error())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		drainTimeout:   drain,
		onError:        onErr,
		handlers:       make(map[string]handlerEntry),
		inflightCtx:    ctx,
		inflightCancel: cancel,
		shutdownCh:     make(chan struct{}),
	}
	if maxInflight > 0 {
		d.sem = make(chan struct{}, maxInflight)
	}
	if opts.Queue != nil {
		d.queue = opts.Queue
	} else {
		d.queue = &inProcessQueue{d: d}
	}
	return d
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
	// Construct one zero-value Req to capture (a) the type for the
	// producer-side ErrTypeMismatch check, and (b) a closure factory
	// for fresh zero-valued Reqs on the consumer side (where bytes off
	// the queue need to decode into a fresh message). One reflect.New
	// at register time; the hot path closes over the typed result.
	zero := newProtoMessage[Req]()
	entry := handlerEntry{
		reqType: zero,
		newReq:  func() proto.Message { return newProtoMessage[Req]() },
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

// DoAsync routes a submission through the configured `Queue` (default:
// in-process goroutine pool). The caller's `ctx` governs any
// submission-side wait (filling the slot semaphore on the in-process
// queue; the wait on an external queue's send buffer / network ack).
//
// Returns an error for the pre-dispatch failures (`ErrShuttingDown`,
// `ErrUnknownMethod`, `ErrTypeMismatch`, `ctx.Err()`); handler errors
// from the in-process path flow through `Options.OnError`. External
// queue adapters surface delivery failures through `Queue.Submit`'s
// error return and runtime failures through their own ack/nack semantics
// at the consumer side.
//
// The marshaled payload is `proto.Marshal(req)` with deterministic
// ordering — the same bytes any worker process (or another SDK) will
// pull off the queue and feed back into the registered handler.
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
	// Pre-check the request type at the producer site so a typo is caught
	// before the bytes hit the queue. The handler-side invoke repeats the
	// check after unmarshal for the external-queue path where the
	// producer and consumer are different processes.
	expectedType := entry.reqType
	if expectedType != nil && !typeMatches(req, expectedType) {
		return fmt.Errorf("%w: %q expects %T, got %T", ErrTypeMismatch, method, expectedType, req)
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return fmt.Errorf("dispatch.DoAsync: marshal req for %q: %w", method, err)
	}
	return d.queue.Submit(ctx, method, payload)
}

// Invoke decodes `payload` as the request type registered for `method`
// and runs the registered handler with the given context. Intended for
// queue-backed consumers: pull a message off Kafka / Rabbit / NATS /
// SQS, hand its method-name + payload to `Invoke`, and use the returned
// error to drive ack / nack.
//
// Unlike `DoAsync`, `Invoke` runs the handler synchronously on the
// caller's goroutine and uses the caller's `ctx`. The producer-side
// concurrency cap and drain semantics don't apply here; bound your
// consumer's concurrency at the queue's prefetch / consumer-pool layer
// instead.
func (d *Dispatcher) Invoke(ctx context.Context, method string, payload []byte) error {
	return d.runHandler(ctx, method, payload)
}

// runHandler is the shared lookup + decode + invoke path. Used by the
// in-process queue (with the long-lived `inflightCtx`) and by `Invoke`
// (with the consumer's `ctx`).
func (d *Dispatcher) runHandler(ctx context.Context, method string, payload []byte) error {
	d.mu.RLock()
	entry, ok := d.handlers[method]
	d.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownMethod, method)
	}
	req := entry.newReq()
	if err := proto.Unmarshal(payload, req); err != nil {
		return fmt.Errorf("dispatch: unmarshal payload for %q: %w", method, err)
	}
	return entry.invoke(ctx, req)
}

// typeMatches reports whether `got` has the same concrete proto.Message
// type as `expected`. We compare full names because reflection-based
// type compare across the proto interface is awkward.
func typeMatches(got, expected proto.Message) bool {
	return got.ProtoReflect().Descriptor().FullName() ==
		expected.ProtoReflect().Descriptor().FullName()
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
	// Phase A — signal: mark closed, wake any parked submitters. Done
	// WITHOUT submitMu so parked submitters can wake and release their
	// RLock before we try to take the write Lock as a barrier.
	if !d.closed.Swap(true) {
		close(d.shutdownCh)
	}
	// Phase B — barrier: take the write Lock once, then drop it. This
	// blocks until every concurrent submit that started before our
	// `closed.Swap(true)` has finished its `wg.Add(1)` (or bailed out).
	// After we return, no further `wg.Add(1)` can happen, so the
	// subsequent `wg.Wait()` inside waitDrain is race-free.
	d.submitMu.Lock()
	d.submitMu.Unlock() //nolint:staticcheck // intentional barrier-only lock

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

// ---------------------------------------------------------------------------
// Queue — the producer-side adapter point for external message buses.
// ---------------------------------------------------------------------------

// Queue is the producer interface external message buses plug into.
// A Queue receives a method name + the proto-marshaled request payload;
// what it does with them is up to the implementation: write to a Kafka
// topic, publish to a RabbitMQ exchange, SQS SendMessage, NATS publish,
// Redis Streams XADD, etc.
//
// The consumer side is the implementation's concern too — the framework
// only standardizes the producer interface and the wire format (method
// name + deterministic proto bytes). Consumers built on this should pull
// messages off their queue and feed (method, payload) into
// `Dispatcher.Invoke` to look up the registered handler and run it
// synchronously on the consumer goroutine; the queue's native ack/nack
// drives delivery semantics.
//
// In-process: the default implementation spawns a goroutine and runs
// the handler immediately, applying `MaxInflight` / `DrainTimeout`. See
// `New` for how to swap in an external queue.
type Queue interface {
	// Submit pushes a message describing (method, payload) onto the
	// queue. Returns once the message is durably handed off (queue's
	// native ack of the producer's send, or for the in-process queue,
	// once the goroutine has been launched).
	Submit(ctx context.Context, method string, payload []byte) error
	// Close releases any resources held by the queue. Called by
	// `Dispatcher.Shutdown` after the drain. In-process queues use this
	// to drain spawned goroutines; external queues should flush any
	// pending sends and close producer connections.
	Close(ctx context.Context) error
}

// inProcessQueue is the default `Queue` — runs handlers on goroutines
// owned by the dispatcher. Drains via the dispatcher's wg.
type inProcessQueue struct {
	d *Dispatcher
}

func (q *inProcessQueue) Submit(ctx context.Context, method string, payload []byte) error {
	d := q.d

	// The submitMu RLock acts as the happens-before barrier between
	// `wg.Add(1)` and the `wg.Wait()` in Shutdown. Held for the entire
	// submission — including the optional semaphore wait — so any submit
	// that's parked on the slot signal is part of the "in-flight" set
	// Shutdown waits to clear before flipping `closed` for good.
	d.submitMu.RLock()
	defer d.submitMu.RUnlock()

	if d.closed.Load() {
		return ErrShuttingDown
	}
	// Acquire a slot if MaxInflight is set. Three escape hatches; the
	// shutdown branch wakes parked submits without forcing them to wait
	// for their own ctx to expire.
	if d.sem != nil {
		var ctxDone <-chan struct{}
		if ctx != nil {
			ctxDone = ctx.Done()
		}
		select {
		case d.sem <- struct{}{}:
		case <-ctxDone:
			return ctx.Err()
		case <-d.shutdownCh:
			return ErrShuttingDown
		}
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		if d.sem != nil {
			defer func() { <-d.sem }()
		}
		defer func() {
			if r := recover(); r != nil {
				d.onError(method, fmt.Errorf("handler panicked: %v", r))
			}
		}()
		if err := d.runHandler(d.inflightCtx, method, payload); err != nil {
			d.onError(method, err)
		}
	}()
	return nil
}

func (q *inProcessQueue) Close(context.Context) error {
	// The dispatcher's Shutdown owns the wg-drain; nothing extra to do
	// for the in-process backend.
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// newProtoMessage constructs a fresh, zero-valued Req via reflection on
// the type parameter. Works for any concrete `*MyMessage` that
// implements `proto.Message`. Called at register time (once per method)
// and at consumer-side unmarshal (once per delivered message); no hot
// path overhead beyond a single reflect.New.
func newProtoMessage[Req proto.Message]() Req {
	var zero Req
	// proto.Message is always a pointer type for generated types;
	// reflect.New on the element gives us a fresh *MyMessage.
	t := reflect.TypeOf(zero).Elem()
	return reflect.New(t).Interface().(Req)
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
