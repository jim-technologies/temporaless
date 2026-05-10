package dependencies_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/dependencies"
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

func seedCompleted(t *testing.T, store *storage.OpenDALStore, runID, value string) {
	t.Helper()
	body := func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String(value), nil
	}
	_, err := workflow.Run(
		context.Background(),
		store,
		&workflow.Options{WorkflowId: "upstream", RunId: runID, CodeVersion: "v1"},
		nil,
		wrapperspb.String("seed"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestWaitForWorkflowReturnsCompletedResult(t *testing.T) {
	store := newStore(t)
	seedCompleted(t, store, "2026-05-04", "AAPL:100")

	got, err := dependencies.WaitForWorkflow(
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "2026-05-04"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetValue() != "AAPL:100" {
		t.Fatalf("got %q, want %q", got.GetValue(), "AAPL:100")
	}
}

func TestWaitForWorkflowReturnsPendingWhenUpstreamMissing(t *testing.T) {
	store := newStore(t)

	_, err := dependencies.WaitForWorkflow(
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "missing"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
	)
	var pending *workflow.WorkflowDependencyPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("expected *WorkflowDependencyPendingError, got %T (%v)", err, err)
	}
	if pending.WorkflowID != "upstream" || pending.RunID != "missing" {
		t.Fatalf("ID mismatch: workflow_id=%q run_id=%q", pending.WorkflowID, pending.RunID)
	}
	if !errors.Is(err, workflow.ErrWorkflowDependencyPending) {
		t.Fatalf("error should unwrap to ErrWorkflowDependencyPending")
	}
}

func TestWaitForWorkflowReturnsFailedWhenUpstreamFailed(t *testing.T) {
	store := newStore(t)

	body := func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, errors.New("upstream broke")
	}
	_, runErr := workflow.Run(
		context.Background(),
		store,
		&workflow.Options{WorkflowId: "upstream", RunId: "2026-05-04", CodeVersion: "v1"},
		nil,
		wrapperspb.String("seed"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	)
	if runErr == nil {
		t.Fatal("expected the workflow body to fail")
	}

	_, err := dependencies.WaitForWorkflow(
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "2026-05-04"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
	)
	var failed *workflow.WorkflowDependencyFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("expected *WorkflowDependencyFailedError, got %T (%v)", err, err)
	}
}

func TestWaitForWorkflowInsideWorkflowBodyReplays(t *testing.T) {
	store := newStore(t)
	seedCompleted(t, store, "2026-05-04", "AAPL:100")

	calls := 0
	downstream := func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		calls++
		wf, ok := workflow.Current(ctx)
		if !ok {
			t.Fatal("Current(ctx) should be set inside a workflow body")
		}
		upstream, err := dependencies.WaitForWorkflow(
			ctx,
			wf.Store(),
			storage.NewWorkflowKey("upstream", request.GetValue()),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		)
		if err != nil {
			return nil, err
		}
		return wrapperspb.String("signal(" + upstream.GetValue() + ")"), nil
	}

	first, err := workflow.Run(
		context.Background(),
		store,
		&workflow.Options{WorkflowId: "signal", RunId: "2026-05-04", CodeVersion: "v1"},
		nil,
		wrapperspb.String("2026-05-04"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		downstream,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetValue() != "signal(AAPL:100)" {
		t.Fatalf("first = %q", first.GetValue())
	}
	if calls != 1 {
		t.Fatalf("body invocations = %d, want 1", calls)
	}

	// Replay: workflow record exists, body doesn't re-execute.
	second, err := workflow.Run(
		context.Background(),
		store,
		&workflow.Options{WorkflowId: "signal", RunId: "2026-05-04", CodeVersion: "v1"},
		nil,
		wrapperspb.String("2026-05-04"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		downstream,
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.GetValue() != "signal(AAPL:100)" {
		t.Fatalf("second = %q", second.GetValue())
	}
	if calls != 1 {
		t.Fatalf("body re-executed on replay (calls=%d)", calls)
	}
}

func TestWaitForWorkflowValidatesArgs(t *testing.T) {
	_, err := dependencies.WaitForWorkflow[*wrapperspb.StringValue](
		context.Background(),
		nil,
		storage.NewWorkflowKey("upstream", "2026-05-04"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
	)
	if err == nil || err.Error() != "store is required" {
		t.Fatalf("expected store required error, got %v", err)
	}
	store := newStore(t)
	_, err = dependencies.WaitForWorkflow[*wrapperspb.StringValue](
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "2026-05-04"),
		nil,
	)
	if err == nil || err.Error() != "newResult is required" {
		t.Fatalf("expected newResult required error, got %v", err)
	}
}
