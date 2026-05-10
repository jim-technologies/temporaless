package backfill_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/backfill"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}

// invokeWorkflow is the canonical "invoke a Temporaless workflow per run_id"
// helper used by Backfill. Workflow body is parameterized so tests can drive
// different scenarios.
func invokeWorkflow(
	store storage.Store,
	body workflow.WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue],
) backfill.Invoke[*wrapperspb.StringValue] {
	return func(ctx context.Context, runID string) (*wrapperspb.StringValue, error) {
		return workflow.Run(
			ctx,
			store,
			&workflow.Options{WorkflowId: "prices", RunId: runID, CodeVersion: "v1"},
			nil,
			wrapperspb.String(runID),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			body,
		)
	}
}

func TestBackfillRunsAllRunIDs(t *testing.T) {
	store := newStore(t)
	body := func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("price:" + request.GetValue()), nil
	}

	report, err := backfill.Backfill(
		context.Background(),
		[]string{"2026-05-01", "2026-05-02", "2026-05-03"},
		backfill.Options{Concurrency: 1},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(report.Succeeded()), 3; got != want {
		t.Fatalf("succeeded = %d, want %d (report=%s)", got, want, report)
	}
	for _, entry := range report.Entries {
		if entry.Result.GetValue() != "price:"+entry.RunID {
			t.Fatalf("entry %s: result = %q", entry.RunID, entry.Result.GetValue())
		}
	}
}

func TestBackfillReplaysAlreadyCompletedRuns(t *testing.T) {
	store := newStore(t)
	var calls atomic.Int64
	body := func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		calls.Add(1)
		return wrapperspb.String("price:" + request.GetValue()), nil
	}
	runIDs := []string{"2026-05-01", "2026-05-02"}

	first, err := backfill.Backfill(
		context.Background(),
		runIDs,
		backfill.Options{Concurrency: 1},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Succeeded()) != 2 {
		t.Fatalf("first pass = %s", first)
	}
	if calls.Load() != 2 {
		t.Fatalf("first pass calls = %d, want 2", calls.Load())
	}

	// Second pass: storage replay short-circuits the body.
	second, err := backfill.Backfill(
		context.Background(),
		runIDs,
		backfill.Options{Concurrency: 1},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Succeeded()) != 2 {
		t.Fatalf("second pass = %s", second)
	}
	if calls.Load() != 2 {
		t.Fatalf("second pass re-fired the body (calls=%d)", calls.Load())
	}
}

func TestBackfillRespectsConcurrencyLimit(t *testing.T) {
	store := newStore(t)
	var inFlight atomic.Int32
	var peak atomic.Int32
	var mu sync.Mutex

	body := func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		current := inFlight.Add(1)
		defer inFlight.Add(-1)
		mu.Lock()
		if current > peak.Load() {
			peak.Store(current)
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		return wrapperspb.String(request.GetValue()), nil
	}

	runIDs := make([]string, 10)
	for i := range runIDs {
		runIDs[i] = string(rune('a' + i))
	}

	report, err := backfill.Backfill(
		context.Background(),
		runIDs,
		backfill.Options{Concurrency: 3},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Succeeded()) != 10 {
		t.Fatalf("succeeded = %d", len(report.Succeeded()))
	}
	if peak.Load() > 3 {
		t.Fatalf("peak in-flight = %d, want <= 3", peak.Load())
	}
}

func TestBackfillContinuesPastFailuresByDefault(t *testing.T) {
	store := newStore(t)
	body := func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if request.GetValue() == "BAD" {
			return nil, errors.New("upstream broke")
		}
		return wrapperspb.String("price:" + request.GetValue()), nil
	}

	report, err := backfill.Backfill(
		context.Background(),
		[]string{"GOOD-1", "BAD", "GOOD-2"},
		backfill.Options{Concurrency: 1},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(report.Succeeded()); got != 2 {
		t.Fatalf("succeeded = %d, want 2", got)
	}
	if got := len(report.Failed()); got != 1 {
		t.Fatalf("failed = %d, want 1", got)
	}
	if report.Failed()[0].RunID != "BAD" {
		t.Fatalf("failed entry = %q, want BAD", report.Failed()[0].RunID)
	}
}

func TestBackfillHaltOnErrorStopsAfterFirstFailure(t *testing.T) {
	store := newStore(t)
	body := func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if request.GetValue() == "BAD" {
			return nil, errors.New("upstream broke")
		}
		return wrapperspb.String("ok:" + request.GetValue()), nil
	}

	report, err := backfill.Backfill(
		context.Background(),
		[]string{"BAD", "after-1", "after-2", "after-3"},
		backfill.Options{Concurrency: 1, HaltOnError: true},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Failed()) != 1 || report.Failed()[0].RunID != "BAD" {
		t.Fatalf("expected single BAD failure, got %s", report)
	}
	if len(report.Pending()) < 1 {
		t.Fatalf("expected at least one pending after halt, got %s", report)
	}
	if got := len(report.Entries); got != 4 {
		t.Fatalf("total entries = %d, want 4", got)
	}
}

func TestBackfillReportsPendingForTimerPending(t *testing.T) {
	// A workflow body that tries to sleep — the first invocation returns
	// TimerPendingError. Backfill should report PENDING, not FAILED.
	store := newStore(t)
	body := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := workflow.Sleep(ctx, "wait", time.Hour); err != nil {
			return nil, err
		}
		return wrapperspb.String("never"), nil
	}

	report, err := backfill.Backfill(
		context.Background(),
		[]string{"2026-05-01", "2026-05-02"},
		backfill.Options{Concurrency: 1},
		invokeWorkflow(store, body),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(report.Pending()); got != 2 {
		t.Fatalf("pending = %d, want 2 (report=%s)", got, report)
	}
	if len(report.Failed()) != 0 || len(report.Succeeded()) != 0 {
		t.Fatalf("expected no failed/succeeded, got %s", report)
	}
}

func TestBackfillTreatsConnectUnavailableAsPending(t *testing.T) {
	// HandleConnect maps TimerPendingError to *connect.Error{Unavailable}.
	// When the invoke layer returns that, backfill should still classify it
	// as Pending.
	report, err := backfill.Backfill[*wrapperspb.StringValue](
		context.Background(),
		[]string{"x", "y"},
		backfill.Options{Concurrency: 1},
		func(_ context.Context, _ string) (*wrapperspb.StringValue, error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("timer pending"))
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Pending()) != 2 {
		t.Fatalf("pending = %d, want 2", len(report.Pending()))
	}
}

func TestBackfillRequiresInvokeAndSensibleConcurrency(t *testing.T) {
	if _, err := backfill.Backfill[*wrapperspb.StringValue](
		context.Background(),
		[]string{"x"},
		backfill.Options{Concurrency: 1},
		nil,
	); err == nil {
		t.Fatal("expected error when invoke is nil")
	}
	if _, err := backfill.Backfill(
		context.Background(),
		[]string{"x"},
		backfill.Options{Concurrency: -1},
		func(_ context.Context, _ string) (*wrapperspb.StringValue, error) { return nil, nil },
	); err == nil {
		t.Fatal("expected error for negative concurrency")
	}
}

func TestBackfillEmptyRunIDsReturnsEmptyReport(t *testing.T) {
	report, err := backfill.Backfill[*wrapperspb.StringValue](
		context.Background(),
		nil,
		backfill.Options{Concurrency: 1},
		func(_ context.Context, _ string) (*wrapperspb.StringValue, error) {
			t.Fatal("invoke should not be called for empty run_ids")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(report.Entries))
	}
}
