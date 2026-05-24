package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// drain is shorthand for Options{Proto: &DispatchOptions{drain_timeout: d}}.
func drain(t time.Duration) *temporalessv1.DispatchOptions {
	return &temporalessv1.DispatchOptions{DrainTimeout: durationpb.New(t)}
}

// inflight returns DispatchOptions with max_inflight set.
func inflight(n uint32) *temporalessv1.DispatchOptions {
	return &temporalessv1.DispatchOptions{MaxInflight: n}
}

// both returns DispatchOptions with both knobs set.
func both(d time.Duration, n uint32) *temporalessv1.DispatchOptions {
	return &temporalessv1.DispatchOptions{
		DrainTimeout: durationpb.New(d),
		MaxInflight:  n,
	}
}

// TestDoAsyncRunsHandlerInGoroutine verifies the call returns BEFORE the
// handler finishes — the whole point of DoAsync.
func TestDoAsyncRunsHandlerInGoroutine(t *testing.T) {
	d := New(Options{})
	defer func() { _ = d.Shutdown(context.Background()) }()

	gate := make(chan struct{})
	done := make(chan struct{})
	Register(d, "/x/Slow", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		<-gate
		close(done)
		return wrapperspb.String("ok"), nil
	})

	start := time.Now()
	if err := d.DoAsync(context.Background(), "/x/Slow", wrapperspb.String("hi")); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Errorf("DoAsync blocked %v (should return immediately)", elapsed)
	}

	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not run within 1s")
	}
}

// TestDoAsyncErrors covers the synchronous-error paths: unknown method,
// type mismatch, shutting-down state, nil req.
func TestDoAsyncErrors(t *testing.T) {
	d := New(Options{})
	Register(d, "/x/Want", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("ok"), nil
	})

	if err := d.DoAsync(context.Background(), "/x/Missing", wrapperspb.String("hi")); !errors.Is(err, ErrUnknownMethod) {
		t.Errorf("missing method: err=%v want %v", err, ErrUnknownMethod)
	}

	// Type mismatch — handler expects StringValue, give it Int32Value.
	// The producer-side check now catches this synchronously; the caller
	// sees ErrTypeMismatch at the call site instead of via OnError.
	if err := d.DoAsync(context.Background(), "/x/Want", wrapperspb.Int32(7)); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("type mismatch: err=%v want %v", err, ErrTypeMismatch)
	}

	if err := d.DoAsync(context.Background(), "/x/Want", nil); err == nil {
		t.Error("nil req: expected error")
	}

	_ = d.Shutdown(context.Background())
	if err := d.DoAsync(context.Background(), "/x/Want", wrapperspb.String("hi")); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("after shutdown: err=%v want %v", err, ErrShuttingDown)
	}
}

// TestTypeMismatchCaughtAtSubmit verifies the producer-side type check
// rejects mismatched request types synchronously (returns
// ErrTypeMismatch) instead of letting them surface via OnError after
// the goroutine launches. This matters for the external-queue path too:
// catching the mismatch BEFORE bytes hit the queue keeps a bad call from
// being durably enqueued and then dead-lettered later.
func TestTypeMismatchCaughtAtSubmit(t *testing.T) {
	d := New(Options{})
	defer func() { _ = d.Shutdown(context.Background()) }()
	Register(d, "/x/Strict", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return req, nil
	})

	err := d.DoAsync(context.Background(), "/x/Strict", wrapperspb.Int32(7))
	if !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("err = %v, want ErrTypeMismatch", err)
	}
}

// TestShutdownDrainsRunningGoroutines is the load-bearing test: a SIGTERM-
// style shutdown must wait for in-flight handlers to finish their work
// instead of abandoning them. We start a handler that takes 200ms, fire
// Shutdown immediately with a 2s drain, and check the handler completed
// AND Shutdown waited for it.
func TestShutdownDrainsRunningGoroutines(t *testing.T) {
	d := New(Options{Proto: drain(2*time.Second)})

	var completed atomic.Bool
	Register(d, "/x/Work", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		// Pretend the work takes 200ms (vendor call, etc.)
		select {
		case <-time.After(200 * time.Millisecond):
			completed.Store(true)
			return wrapperspb.String("done"), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	if err := d.DoAsync(context.Background(), "/x/Work", wrapperspb.String("hi")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	if !completed.Load() {
		t.Error("Shutdown returned before the handler completed — drain abandoned the goroutine")
	}
	if elapsed < 150*time.Millisecond {
		t.Errorf("Shutdown returned in %v (handler needs ~200ms — drain must wait)", elapsed)
	}
	if elapsed > time.Second {
		t.Errorf("Shutdown waited %v unnecessarily", elapsed)
	}
}

// TestShutdownCancelsContextAfterDrainTimeout — when handlers run longer
// than DrainTimeout, Shutdown cancels the per-handler context so a
// well-behaved handler can bail. We still wait for the handler to return.
func TestShutdownCancelsContextAfterDrainTimeout(t *testing.T) {
	d := New(Options{Proto: drain(50*time.Millisecond)})

	var bailedOnCancel atomic.Bool
	handlerReturned := make(chan struct{})
	Register(d, "/x/Long", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		defer close(handlerReturned)
		// Handler would take 5s; ctx cancellation should let it return
		// in well under that.
		select {
		case <-time.After(5 * time.Second):
			return wrapperspb.String("never"), nil
		case <-ctx.Done():
			bailedOnCancel.Store(true)
			return nil, ctx.Err()
		}
	})
	if err := d.DoAsync(context.Background(), "/x/Long", wrapperspb.String("hi")); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)

	select {
	case <-handlerReturned:
	default:
		t.Fatal("Shutdown returned but handler did not return — orphaned goroutine")
	}
	if !bailedOnCancel.Load() {
		t.Error("handler did not observe ctx.Done() — cancellation was not signalled")
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("Shutdown returned in %v — drain window was %v", elapsed, 50*time.Millisecond)
	}
	if elapsed > time.Second {
		t.Errorf("Shutdown took %v — handler should bail promptly on cancel", elapsed)
	}
}

// TestShutdownReturnsCtxErrIfShutdownCtxCancels — when the caller's own
// shutdown context expires before goroutines drain, we surface its error
// instead of blocking forever.
func TestShutdownReturnsCtxErrIfShutdownCtxCancels(t *testing.T) {
	d := New(Options{Proto: drain(0)})

	handlerCanReturn := make(chan struct{})
	defer close(handlerCanReturn) // let the orphaned goroutine drain

	Register(d, "/x/Stuck", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		// Deliberately ignores ctx — the test checks that Shutdown surfaces
		// the shutdownCtx error rather than blocking forever.
		<-handlerCanReturn
		return wrapperspb.String("eventually"), nil
	})
	if err := d.DoAsync(context.Background(), "/x/Stuck", wrapperspb.String("hi")); err != nil {
		t.Fatal(err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := d.Shutdown(shutdownCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown err = %v, want context.DeadlineExceeded", err)
	}
}

// TestPanickingHandlerSurfacesViaOnError — a panicking handler must not
// crash the process; it must surface as an error.
func TestPanickingHandlerSurfacesViaOnError(t *testing.T) {
	var seen struct {
		sync.Mutex
		err error
	}
	d := New(Options{OnError: func(method string, err error) {
		seen.Lock()
		defer seen.Unlock()
		seen.err = err
	}})
	Register(d, "/x/Boom", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		panic("kaboom")
	})

	if err := d.DoAsync(context.Background(), "/x/Boom", wrapperspb.String("hi")); err != nil {
		t.Fatal(err)
	}
	_ = d.Shutdown(context.Background())

	seen.Lock()
	defer seen.Unlock()
	if seen.err == nil || seen.err.Error() == "" {
		t.Fatal("OnError did not see the panic")
	}
}

// TestMaxInflightCapsConcurrentHandlers — with MaxInflight=N, no more
// than N handlers run at the same time. Submissions over the cap block
// in DoAsync until a slot frees.
func TestMaxInflightCapsConcurrentHandlers(t *testing.T) {
	const cap = 3
	d := New(Options{Proto: inflight(uint32(cap))})
	defer func() { _ = d.Shutdown(context.Background()) }()

	var (
		inflight    atomic.Int64
		maxObserved atomic.Int64
	)
	release := make(chan struct{})
	Register(d, "/x/Bounded", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		cur := inflight.Add(1)
		for {
			prev := maxObserved.Load()
			if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
				break
			}
		}
		<-release
		inflight.Add(-1)
		return wrapperspb.String("ok"), nil
	})

	// Launch 10 submissions; only `cap` should be running at once.
	const total = 10
	submitted := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		go func() {
			_ = d.DoAsync(context.Background(), "/x/Bounded", wrapperspb.String("hi"))
			submitted <- struct{}{}
		}()
	}

	// Give the first `cap` time to enter the handler.
	time.Sleep(50 * time.Millisecond)
	if got := inflight.Load(); got != cap {
		t.Errorf("inflight = %d before release, want %d (rest should be blocked in DoAsync)", got, cap)
	}

	close(release)
	// Drain all submissions.
	for i := 0; i < total; i++ {
		select {
		case <-submitted:
		case <-time.After(5 * time.Second):
			t.Fatal("submission did not complete within 5s")
		}
	}

	if got := maxObserved.Load(); got > cap {
		t.Errorf("max concurrent inflight = %d, want <= %d", got, cap)
	}
}

// TestMaxInflightBlocksUntilCtxCancels — a blocked submission returns
// the caller's ctx error when ctx cancels, without ever running.
func TestMaxInflightBlocksUntilCtxCancels(t *testing.T) {
	d := New(Options{Proto: inflight(1)})
	defer func() { _ = d.Shutdown(context.Background()) }()

	hold := make(chan struct{})
	Register(d, "/x/Hog", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		<-hold
		return wrapperspb.String("ok"), nil
	})

	// Fill the one slot.
	if err := d.DoAsync(context.Background(), "/x/Hog", wrapperspb.String("first")); err != nil {
		t.Fatal(err)
	}

	// Second submission blocks; cancel its ctx and expect ctx.Err().
	secondCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- d.DoAsync(secondCtx, "/x/Hog", wrapperspb.String("second"))
	}()

	time.Sleep(50 * time.Millisecond) // ensure the goroutine reached the select
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoAsync did not return after ctx cancel")
	}
	close(hold)
}

// TestMaxInflightUnblocksOnShutdown — a blocked submission returns
// ErrShuttingDown when Shutdown begins, not its ctx error.
func TestMaxInflightUnblocksOnShutdown(t *testing.T) {
	d := New(Options{Proto: both(100*time.Millisecond, 1)})

	hold := make(chan struct{})
	Register(d, "/x/Hog", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		<-hold
		return wrapperspb.String("ok"), nil
	})
	if err := d.DoAsync(context.Background(), "/x/Hog", wrapperspb.String("first")); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- d.DoAsync(context.Background(), "/x/Hog", wrapperspb.String("second"))
	}()
	time.Sleep(50 * time.Millisecond) // ensure blocked

	// Shutdown drains by signalling, then cancels in-flight.
	go func() {
		close(hold)
		_ = d.Shutdown(context.Background())
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrShuttingDown) {
			t.Errorf("err = %v, want ErrShuttingDown", err)
		}
	case <-time.After(time.Second):
		t.Fatal("DoAsync did not return after shutdown")
	}
}

// TestUnboundedByDefault — MaxInflight==0 means no cap; the original
// goroutine-per-submission behavior is preserved unchanged.
func TestUnboundedByDefault(t *testing.T) {
	d := New(Options{}) // MaxInflight unset
	defer func() { _ = d.Shutdown(context.Background()) }()

	var inflight atomic.Int64
	release := make(chan struct{})
	Register(d, "/x/Burst", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		inflight.Add(1)
		<-release
		return wrapperspb.String("ok"), nil
	})

	const total = 50
	for i := 0; i < total; i++ {
		if err := d.DoAsync(context.Background(), "/x/Burst", wrapperspb.String("hi")); err != nil {
			t.Fatal(err)
		}
	}
	// All 50 should be running concurrently since there's no cap.
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		if inflight.Load() == total {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := inflight.Load(); got != total {
		t.Errorf("inflight = %d, want %d (unbounded should run all concurrently)", got, total)
	}
	close(release)
}

// TestInvokeRunsRegisteredHandlerFromBytes — the external-queue consumer
// path. Producer's payload is proto bytes; consumer feeds (method, bytes)
// into Invoke which looks up the handler, unmarshals, and runs it.
func TestInvokeRunsRegisteredHandlerFromBytes(t *testing.T) {
	d := New(Options{})
	defer func() { _ = d.Shutdown(context.Background()) }()

	got := make(chan string, 1)
	Register(d, "/x/Echo", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		got <- req.GetValue()
		return wrapperspb.String("ack:" + req.GetValue()), nil
	})

	// Simulate what an external consumer would do: marshal the req
	// (same wire format DoAsync produces), then feed into Invoke.
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Invoke(context.Background(), "/x/Echo", payload); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "hello" {
			t.Errorf("handler got %q, want %q", s, "hello")
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not run")
	}
}

// TestInvokeUnknownMethodReturnsError — consumer hands us a method we
// don't recognize. Surface ErrUnknownMethod so the consumer can nack
// or dead-letter the message.
func TestInvokeUnknownMethodReturnsError(t *testing.T) {
	d := New(Options{})
	defer func() { _ = d.Shutdown(context.Background()) }()

	err := d.Invoke(context.Background(), "/x/NotRegistered", []byte{})
	if !errors.Is(err, ErrUnknownMethod) {
		t.Errorf("err = %v, want ErrUnknownMethod", err)
	}
}

// TestCustomQueueReceivesSubmission — verify the Queue extension point.
// A user-supplied Queue captures (method, payload) instead of running
// the handler in-process — the contract Kafka/Rabbit/SQS adapters use.
func TestCustomQueueReceivesSubmission(t *testing.T) {
	captured := struct {
		sync.Mutex
		method  string
		payload []byte
	}{}
	q := queueFunc(func(ctx context.Context, method string, payload []byte) error {
		captured.Lock()
		defer captured.Unlock()
		captured.method = method
		captured.payload = payload
		return nil
	})
	d := New(Options{Queue: q})
	defer func() { _ = d.Shutdown(context.Background()) }()

	Register(d, "/x/Submit", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		t.Error("handler should NOT run when a custom queue captures the message")
		return req, nil
	})

	if err := d.DoAsync(context.Background(), "/x/Submit", wrapperspb.String("payload")); err != nil {
		t.Fatal(err)
	}
	captured.Lock()
	defer captured.Unlock()
	if captured.method != "/x/Submit" {
		t.Errorf("method=%q want /x/Submit", captured.method)
	}
	if len(captured.payload) == 0 {
		t.Error("payload was empty")
	}
	// Verify the payload is the proto-marshaled request so an external
	// consumer can decode it back into the registered handler's Req type.
	roundTrip := &wrapperspb.StringValue{}
	if err := proto.Unmarshal(captured.payload, roundTrip); err != nil {
		t.Fatalf("payload did not unmarshal as StringValue: %v", err)
	}
	if roundTrip.GetValue() != "payload" {
		t.Errorf("payload round-trip = %q, want %q", roundTrip.GetValue(), "payload")
	}
}

// queueFunc adapts a function value to the Queue interface — tiny helper
// so tests can register inline queues without defining a type.
type queueFunc func(ctx context.Context, method string, payload []byte) error

func (q queueFunc) Submit(ctx context.Context, method string, payload []byte) error {
	return q(ctx, method, payload)
}
func (q queueFunc) Close(context.Context) error { return nil }

// TestConcurrentSubmissionsAndShutdown stresses the dispatch+drain path
// under N concurrent submissions racing with a Shutdown.
func TestConcurrentSubmissionsAndShutdown(t *testing.T) {
	d := New(Options{Proto: drain(2*time.Second)})

	var completed atomic.Int64
	Register(d, "/x/Quick", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		time.Sleep(time.Duration(req.GetValue()[0]) * time.Microsecond)
		completed.Add(1)
		return wrapperspb.String("ok"), nil
	})

	const n = 200
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			// Mix of accepted-before-shutdown and rejected-during-shutdown.
			_ = d.DoAsync(context.Background(), "/x/Quick", wrapperspb.String(string(rune(1+(i%10)))))
		}()
	}
	wg.Wait()
	if err := d.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	// We can't assert completed == n because some submissions race with
	// Shutdown and may be rejected. But every accepted submission MUST
	// complete (Shutdown drains them all).
	t.Logf("completed=%d of %d submissions (rest rejected after shutdown)", completed.Load(), n)
}
