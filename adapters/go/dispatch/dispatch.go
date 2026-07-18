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
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DefaultDrainTimeout is the per-phase bound `Shutdown` uses while draining
// admitted producer submissions and in-flight handler goroutines. Chosen to
// fit common SIGTERM grace periods when callers also put an overall deadline
// on the shutdown context.
const DefaultDrainTimeout = 15 * time.Second

// DefaultTaskTTL is how long completed (DONE/FAILED) TaskInfo records stay
// queryable via `Status(taskID)` before the GC sweep evicts them. Chosen
// long enough that a polling client with a 2s interval can comfortably
// observe terminal state after a long-running upload completes, short
// enough that the tracker doesn't grow without bound across a day of
// traffic.
const DefaultTaskTTL = 1 * time.Hour

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

// ErrSubmissionDrainTimeout is returned by Shutdown when an external Queue
// still has a producer Submit call in progress after Queue.Close has returned
// and a second DrainTimeout window has elapsed. Queue implementations should
// make Close release or fail outstanding Submit calls.
var ErrSubmissionDrainTimeout = errors.New("dispatch submissions did not drain")

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

	// OnError is invoked when a handler returns a non-nil error. Receives
	// the gRPC method name, the per-submission task_id (so logs are
	// grep-able and a dashboard can correlate), and the error. Default:
	// log at WARN via `slog.Default()`. Override to integrate with your
	// telemetry pipeline. Used only by the in-process queue path; external
	// queue adapters surface their own errors via the queue's native ack /
	// nack semantics.
	OnError func(method, taskID string, err error)
}

// Dispatcher is a goroutine pool keyed by gRPC-style method names. Submissions
// route through `Queue` (default: in-process). All queues get producer
// admission, bounded Submit draining, and exactly one Close call. Handler
// goroutine draining applies only to the in-process queue; external adapters
// own their remote delivery, retry, and consumer lifecycles.
type Dispatcher struct {
	drainTimeout time.Duration
	taskTTL      time.Duration
	onError      func(method, taskID string, err error)

	// queue is the producer backend. Defaults to inProcessQueue.
	queue Queue

	mu       sync.RWMutex
	handlers map[string]handlerEntry

	// tasks is the in-memory tracker map: task_id → TaskInfo.
	// PENDING/RUNNING entries never evict; DONE/FAILED entries evict
	// after taskTTL via the gc loop. Guarded by tasksMu (separate from
	// `mu` so handler invocation lookups don't contend with status
	// queries).
	tasksMu sync.RWMutex
	tasks   map[string]*temporalessv1.TaskInfo

	// gcStop signals the tracker GC goroutine to exit; gcDone closes
	// after it has returned. Shutdown waits on gcDone so a clean exit
	// doesn't leave the goroutine dangling.
	gcStop chan struct{}
	gcDone chan struct{}

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

	// submitMu is a short admission lock. It serializes the final closed check
	// with incrementing activeSubmissions, but is never held while Queue.Submit
	// performs I/O. Shutdown snapshots submissionsDone after closing admission,
	// then waits with shutdownCtx rather than blocking on an uninterruptible
	// mutex. For the in-process queue, Submit returns only after wg.Add(1), so a
	// drained submissionsDone is also the barrier that makes wg.Wait safe.
	submitMu          sync.Mutex
	activeSubmissions int
	submissionsDone   chan struct{}

	shutdownMu      sync.Mutex
	shutdownStarted bool
	shutdownDone    chan struct{}
	shutdownErr     error
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
	// handler. Returns the handler's response (so the tracker can stash
	// it in TaskInfo.response) plus any type-mismatch / handler error.
	invoke func(ctx context.Context, req proto.Message) (proto.Message, error)
}

// New constructs a `Dispatcher`. Pass a zero-value `Options{}` for the
// in-process queue with framework defaults.
func New(opts Options) *Dispatcher {
	drain := opts.Proto.GetDrainTimeout().AsDuration()
	if drain <= 0 {
		drain = DefaultDrainTimeout
	}
	ttl := opts.Proto.GetTaskTtl().AsDuration()
	if ttl <= 0 {
		ttl = DefaultTaskTTL
	}
	maxInflight := int(opts.Proto.GetMaxInflight())
	onErr := opts.OnError
	if onErr == nil {
		onErr = func(method, taskID string, err error) {
			slog.Default().Warn("dispatch: handler returned error",
				"method", method, "task_id", taskID, "err", err.Error())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		drainTimeout:   drain,
		taskTTL:        ttl,
		onError:        onErr,
		handlers:       make(map[string]handlerEntry),
		tasks:          make(map[string]*temporalessv1.TaskInfo),
		inflightCtx:    ctx,
		inflightCancel: cancel,
		shutdownCh:     make(chan struct{}),
		shutdownDone:   make(chan struct{}),
		gcStop:         make(chan struct{}),
		gcDone:         make(chan struct{}),
	}
	if maxInflight > 0 {
		d.sem = make(chan struct{}, maxInflight)
	}
	if opts.Queue != nil {
		d.queue = opts.Queue
	} else {
		d.queue = &inProcessQueue{d: d}
	}
	go d.trackerGC()
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
		invoke: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			typed, ok := req.(Req)
			if !ok {
				return nil, fmt.Errorf("%w: handler %q expects %T, got %T",
					ErrTypeMismatch, method, zero, req)
			}
			resp, err := handle(ctx, typed)
			if err != nil {
				return nil, err
			}
			return resp, nil
		},
	}
	d.mu.Lock()
	d.handlers[method] = entry
	d.mu.Unlock()
}

// DoAsync routes a submission through the configured `Queue` (default:
// in-process goroutine pool). Returns the per-submission task_id (always
// non-empty on success) plus any pre-dispatch error.
//
// The caller's `ctx` governs any submission-side wait (filling the slot
// semaphore on the in-process queue; the wait on an external queue's
// send buffer / network ack).
//
// Pre-dispatch errors (`ErrShuttingDown`, `ErrUnknownMethod`,
// `ErrTypeMismatch`, `ctx.Err()`) return ("", err). Handler errors from
// the in-process path flow through `Options.OnError` and into the task
// record (queryable via `Status(taskID)`). External queue adapters
// surface delivery failures through their own ack/nack semantics on the
// consumer side; for those, the task record stays PENDING until the
// remote worker calls `Invoke`.
//
// The marshaled payload is `proto.Marshal(req)` with deterministic
// ordering — the same bytes any worker process (or another SDK) will
// pull off the queue and feed back into the registered handler.
func (d *Dispatcher) DoAsync(ctx context.Context, method string, req proto.Message) (string, error) {
	if d.closed.Load() {
		return "", ErrShuttingDown
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	d.mu.RLock()
	entry, ok := d.handlers[method]
	d.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("%w: %q", ErrUnknownMethod, method)
	}
	if req == nil {
		return "", fmt.Errorf("dispatch.DoAsync: req is required for method %q", method)
	}
	// Pre-check the request type at the producer site so a typo is caught
	// before the bytes hit the queue. The handler-side invoke repeats the
	// check after unmarshal for the external-queue path where the
	// producer and consumer are different processes.
	expectedType := entry.reqType
	if expectedType != nil && !typeMatches(req, expectedType) {
		return "", fmt.Errorf("%w: %q expects %T, got %T", ErrTypeMismatch, method, expectedType, req)
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("dispatch.DoAsync: marshal req for %q: %w", method, err)
	}
	taskID := newTaskID()
	d.markPending(taskID, method)
	d.submitMu.Lock()
	if d.closed.Load() {
		d.submitMu.Unlock()
		d.markFailed(taskID, ErrShuttingDown)
		return "", ErrShuttingDown
	}
	d.activeSubmissions++
	if d.activeSubmissions == 1 {
		d.submissionsDone = make(chan struct{})
	}
	d.submitMu.Unlock()
	defer d.finishSubmission()
	if err := d.queue.Submit(ctx, method, taskID, payload); err != nil {
		// Submission rejected — surface as a FAILED record so a caller
		// polling Status doesn't see PENDING forever.
		d.markFailed(taskID, err)
		return "", err
	}
	return taskID, nil
}

// Status returns the current TaskInfo for `taskID`, or (nil, false) if
// the id is unknown or has been TTL-evicted. A returned `*TaskInfo` is
// a defensive clone — mutating it has no effect on the dispatcher's
// internal state.
func (d *Dispatcher) Status(taskID string) (*temporalessv1.TaskInfo, bool) {
	d.tasksMu.RLock()
	defer d.tasksMu.RUnlock()
	t, ok := d.tasks[taskID]
	if !ok {
		return nil, false
	}
	return proto.Clone(t).(*temporalessv1.TaskInfo), true
}

// newTaskID generates a fresh ULID for one submission. ULIDs sort by
// time so external observers see a natural arrival ordering.
func newTaskID() string {
	return ulid.MustNew(ulid.Now(), rand.Reader).String()
}

func (d *Dispatcher) markPending(taskID, method string) {
	now := timestamppb.Now()
	d.tasksMu.Lock()
	d.tasks[taskID] = &temporalessv1.TaskInfo{
		TaskId:      taskID,
		Method:      method,
		Status:      temporalessv1.TaskStatus_TASK_STATUS_PENDING,
		SubmittedAt: now,
	}
	d.tasksMu.Unlock()
}

func (d *Dispatcher) markRunning(taskID string) {
	d.tasksMu.Lock()
	defer d.tasksMu.Unlock()
	if t, ok := d.tasks[taskID]; ok {
		t.Status = temporalessv1.TaskStatus_TASK_STATUS_RUNNING
	}
}

func (d *Dispatcher) markDone(taskID string, resp proto.Message) {
	now := timestamppb.Now()
	var any *anypb.Any
	if resp != nil {
		// Best-effort: if Any-wrapping fails (e.g. resp is a non-proto3
		// message somehow), record DONE without a response payload
		// rather than silently FAILing the task.
		if packed, err := anypb.New(resp); err == nil {
			any = packed
		}
	}
	d.tasksMu.Lock()
	defer d.tasksMu.Unlock()
	if t, ok := d.tasks[taskID]; ok {
		t.Status = temporalessv1.TaskStatus_TASK_STATUS_DONE
		t.Response = any
		t.CompletedAt = now
	}
}

func (d *Dispatcher) markFailed(taskID string, err error) {
	now := timestamppb.Now()
	d.tasksMu.Lock()
	defer d.tasksMu.Unlock()
	if t, ok := d.tasks[taskID]; ok {
		t.Status = temporalessv1.TaskStatus_TASK_STATUS_FAILED
		if err != nil {
			t.Error = err.Error()
		}
		t.CompletedAt = now
	}
}

// trackerGC sweeps terminal (DONE/FAILED) task records older than
// `taskTTL`. PENDING/RUNNING records never evict — losing one mid-flight
// would be the worst possible failure mode for an observability tool.
func (d *Dispatcher) trackerGC() {
	defer close(d.gcDone)
	tick := time.NewTicker(d.taskTTL / 2)
	defer tick.Stop()
	for {
		select {
		case <-d.gcStop:
			return
		case <-tick.C:
			d.evictExpiredTasks()
		}
	}
}

func (d *Dispatcher) evictExpiredTasks() {
	cutoff := time.Now().Add(-d.taskTTL)
	d.tasksMu.Lock()
	defer d.tasksMu.Unlock()
	for id, t := range d.tasks {
		if t.CompletedAt == nil {
			continue // PENDING/RUNNING — never evict
		}
		if t.CompletedAt.AsTime().Before(cutoff) {
			delete(d.tasks, id)
		}
	}
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
//
// Invoke does NOT update the tracker — the producer's task_id lives in
// the original dispatcher's memory and is unreachable from a remote
// consumer. External-queue consumers either don't care about tracking
// or wire their own (the queue's native task identity is the right
// primitive).
func (d *Dispatcher) Invoke(ctx context.Context, method string, payload []byte) error {
	_, err := d.runHandler(ctx, method, payload)
	return err
}

// runHandler is the shared lookup + decode + invoke path. Returns the
// handler's response (or nil on any error) so the in-process queue can
// stash it in the tracker.
func (d *Dispatcher) runHandler(ctx context.Context, method string, payload []byte) (proto.Message, error) {
	d.mu.RLock()
	entry, ok := d.handlers[method]
	d.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownMethod, method)
	}
	req := entry.newReq()
	if err := proto.Unmarshal(payload, req); err != nil {
		return nil, fmt.Errorf("dispatch: unmarshal payload for %q: %w", method, err)
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

// Shutdown stops accepting new submissions, gives admitted Queue.Submit calls
// a bounded drain window, closes the queue exactly once, then drains in-flight
// handler goroutines. Handlers still running after `DrainTimeout` receive
// cooperative context cancellation. The caller's `shutdownCtx` is the overall
// bound for every phase.
//
// Returns `context.DeadlineExceeded` (or whatever `shutdownCtx.Err()`
// returns) if `shutdownCtx` cancelled before all goroutines drained.
// Returns nil on clean drain.
//
// Calling `Shutdown` repeatedly or concurrently is safe. One caller performs
// finalization; the rest wait for and receive that same result (unless their
// own shutdownCtx is cancelled first).
func (d *Dispatcher) Shutdown(shutdownCtx context.Context) error {
	d.shutdownMu.Lock()
	if d.shutdownStarted {
		done := d.shutdownDone
		d.shutdownMu.Unlock()
		select {
		case <-done:
			d.shutdownMu.Lock()
			err := d.shutdownErr
			d.shutdownMu.Unlock()
			return err
		case <-shutdownCtx.Done():
			return shutdownCtx.Err()
		}
	}
	d.shutdownStarted = true
	d.shutdownMu.Unlock()

	err := d.shutdown(shutdownCtx)
	d.shutdownMu.Lock()
	d.shutdownErr = err
	close(d.shutdownDone)
	d.shutdownMu.Unlock()
	return err
}

func (d *Dispatcher) shutdown(shutdownCtx context.Context) error {
	// Phase A — signal: mark closed, wake any parked submitters. Done
	// before taking the short admission lock so no later DoAsync can enter.
	d.closed.Store(true)
	close(d.shutdownCh)
	// Phase B — give sends that were admitted before shutdown one bounded
	// window to acknowledge naturally. Do not consume the caller's entire
	// shutdown deadline here: Queue.Close may be the operation that releases
	// a blocked Submit.
	submissionsDrained := d.waitSubmissions(shutdownCtx, d.drainTimeout)

	// Phase C — flush and close the producer while shutdownCtx still has its
	// remaining budget. Queue.Close is called exactly once even when the
	// initial submission window elapsed or the context is already cancelled.
	closeErr := d.queue.Close(shutdownCtx)
	if !submissionsDrained {
		// A well-behaved external queue releases outstanding sends from Close.
		// Give those calls one final bounded window, then surface an explicit
		// error rather than hanging forever when shutdownCtx is Background.
		submissionsDrained = d.waitSubmissions(shutdownCtx, d.drainTimeout)
	}

	var submissionErr error
	if !submissionsDrained {
		submissionErr = ErrSubmissionDrainTimeout
	}

	// Phase 1: best-effort drain. Wait for either the wg to clear or the
	// drain timeout to elapse.
	handlersDrained := submissionsDrained && d.waitDrain(shutdownCtx, d.drainTimeout)
	if !handlersDrained {
		// Drain window expired — signal cancellation to running handlers
		// so they can bail out cooperatively.
		d.inflightCancel()
	}

	// Phase 2: wait for the rest unconditionally, bounded only by
	// shutdownCtx. We never abandon goroutines — orphaning a handler
	// mid-vendor-call is worse than waiting a few extra seconds.
	if submissionsDrained {
		// Waiting on the WaitGroup is safe only after every admitted Submit
		// has returned: the in-process Queue performs wg.Add inside Submit.
		d.waitDrain(shutdownCtx, 0)
	}

	// Phase 3: stop the tracker GC. Done last so any handlers that flipped
	// tasks to DONE/FAILED during drain have already updated the map.
	close(d.gcStop)
	<-d.gcDone

	return errors.Join(submissionErr, shutdownCtx.Err(), closeErr)
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
	// Submit pushes a message describing (method, taskID, payload) onto
	// the queue. Returns once the message is durably handed off (queue's
	// native ack of the producer's send, or for the in-process queue,
	// once the goroutine has been launched). `taskID` is the
	// dispatcher-generated ULID for this submission; external queue
	// implementations should propagate it alongside the payload (e.g.
	// as a message header or attribute) so consumers can correlate.
	Submit(ctx context.Context, method, taskID string, payload []byte) error
	// Close releases any resources held by the queue. Dispatcher.Shutdown
	// calls it exactly once after a bounded wait for admitted Submit calls;
	// if a Submit is still blocked, Close must safely release or fail it.
	// External queues should flush pending sends and close producer
	// connections. The in-process queue is a no-op because the dispatcher
	// drains its handler goroutines separately.
	Close(ctx context.Context) error
}

// inProcessQueue is the default `Queue` — runs handlers on goroutines
// owned by the dispatcher. Drains via the dispatcher's wg.
type inProcessQueue struct {
	d *Dispatcher
}

func (q *inProcessQueue) Submit(ctx context.Context, method, taskID string, payload []byte) error {
	d := q.d

	if d.closed.Load() {
		return ErrShuttingDown
	}
	// Acquire a slot if MaxInflight is set. Three escape hatches; the
	// shutdown branch wakes parked submits without forcing them to wait
	// for their own ctx to expire.
	if d.sem != nil {
		select {
		case d.sem <- struct{}{}:
		case <-ctx.Done():
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
				err := fmt.Errorf("handler panicked: %v", r)
				d.markFailed(taskID, err)
				d.onError(method, taskID, err)
			}
		}()
		d.markRunning(taskID)
		resp, err := d.runHandler(d.inflightCtx, method, payload)
		if err != nil {
			d.markFailed(taskID, err)
			d.onError(method, taskID, err)
			return
		}
		d.markDone(taskID, resp)
	}()
	return nil
}

func (q *inProcessQueue) Close(context.Context) error {
	// The dispatcher's Shutdown owns the wg-drain; nothing extra to do
	// for the in-process backend.
	return nil
}

func (d *Dispatcher) finishSubmission() {
	d.submitMu.Lock()
	defer d.submitMu.Unlock()
	d.activeSubmissions--
	if d.activeSubmissions == 0 {
		close(d.submissionsDone)
		d.submissionsDone = nil
	}
}

// waitSubmissions waits for every Queue.Submit admitted before Shutdown.
// timeout <= 0 means only shutdownCtx bounds the wait.
func (d *Dispatcher) waitSubmissions(shutdownCtx context.Context, timeout time.Duration) bool {
	d.submitMu.Lock()
	done := d.submissionsDone
	d.submitMu.Unlock()
	if done == nil {
		return true
	}
	if timeout <= 0 {
		select {
		case <-done:
			return true
		case <-shutdownCtx.Done():
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	case <-shutdownCtx.Done():
		return false
	}
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

	select {
	case <-done:
		return true
	case <-timerC:
		return false
	case <-shutdownCtx.Done():
		return false
	}
}
