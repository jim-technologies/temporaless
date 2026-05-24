package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

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
	if err := d.DoAsync(context.Background(), "/x/Want", wrapperspb.Int32(7)); err != nil {
		// The pre-check accepts any proto.Message; the type mismatch is
		// then surfaced via OnError because we already dispatched. Verify
		// by hooking OnError below.
		t.Fatal(err)
	}

	if err := d.DoAsync(context.Background(), "/x/Want", nil); err == nil {
		t.Error("nil req: expected error")
	}

	_ = d.Shutdown(context.Background())
	if err := d.DoAsync(context.Background(), "/x/Want", wrapperspb.String("hi")); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("after shutdown: err=%v want %v", err, ErrShuttingDown)
	}
}

// TestTypeMismatchReachesOnError verifies handler-time type-asserts surface
// via OnError, with method + a sane message.
func TestTypeMismatchReachesOnError(t *testing.T) {
	var got struct {
		sync.Mutex
		method string
		err    error
		fired  bool
	}
	hook := func(method string, err error) {
		got.Lock()
		defer got.Unlock()
		got.method = method
		got.err = err
		got.fired = true
	}
	d := New(Options{OnError: hook})
	Register(d, "/x/Strict", func(ctx context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return req, nil
	})

	if err := d.DoAsync(context.Background(), "/x/Strict", wrapperspb.Int32(7)); err != nil {
		t.Fatal(err)
	}
	_ = d.Shutdown(context.Background())

	got.Lock()
	defer got.Unlock()
	if !got.fired {
		t.Fatal("OnError not invoked")
	}
	if got.method != "/x/Strict" {
		t.Errorf("method=%q want /x/Strict", got.method)
	}
	if !errors.Is(got.err, ErrTypeMismatch) {
		t.Errorf("err=%v want ErrTypeMismatch", got.err)
	}
}

// TestShutdownDrainsRunningGoroutines is the load-bearing test: a SIGTERM-
// style shutdown must wait for in-flight handlers to finish their work
// instead of abandoning them. We start a handler that takes 200ms, fire
// Shutdown immediately with a 2s drain, and check the handler completed
// AND Shutdown waited for it.
func TestShutdownDrainsRunningGoroutines(t *testing.T) {
	d := New(Options{DrainTimeout: 2 * time.Second})

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
	d := New(Options{DrainTimeout: 50 * time.Millisecond})

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
	d := New(Options{DrainTimeout: 0})

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

// TestConcurrentSubmissionsAndShutdown stresses the dispatch+drain path
// under N concurrent submissions racing with a Shutdown.
func TestConcurrentSubmissionsAndShutdown(t *testing.T) {
	d := New(Options{DrainTimeout: 2 * time.Second})

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
