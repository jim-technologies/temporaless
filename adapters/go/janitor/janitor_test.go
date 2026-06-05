package janitor_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSweepDeletesOldCompletedRuns(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	// Run 1: completed yesterday — should be swept.
	runWorkflow(t, ctx, store, "prices:old", "2026-05-03")
	backdate(t, ctx, store, "prices:old", "2026-05-03", time.Now().Add(-48*time.Hour))

	// Run 2: completed just now — should be kept.
	runWorkflow(t, ctx, store, "prices:fresh", "2026-05-04")

	// Run 3: still in progress — should be kept.
	leaveInProgress(t, ctx, store, "prices:waiting", "2026-05-04")

	deleted, err := janitor.Sweep(ctx, store, time.Now(), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:old",
		RunID:      "2026-05-03",
	}); found {
		t.Fatal("expected old run to be deleted")
	}
	if _, found, _ := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:old",
		RunID:      "2026-05-03",
		ActivityID: "fetch:AAPL",
	}); found {
		t.Fatal("expected old activity to be deleted")
	}
	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:fresh",
		RunID:      "2026-05-04",
	}); !found {
		t.Fatal("expected fresh run to be kept")
	}
	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:waiting",
		RunID:      "2026-05-04",
	}); !found {
		t.Fatal("expected in-progress run to be kept")
	}
}

func TestSweepRejectsBadInput(t *testing.T) {
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	if _, err := janitor.Sweep(context.Background(), store, time.Now(), 0); err == nil {
		t.Fatal("expected error for non-positive maxAge")
	}
}

func runWorkflow(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string) {
	t.Helper()
	_, err := workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: workflowID, RunId: runID, CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "fetch:" + input.GetValue()},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("ok:" + request.GetValue()), nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func leaveInProgress(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string) {
	t.Helper()
	_, _ = workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: workflowID, RunId: runID, CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := workflow.Sleep(ctx, "wait", time.Hour); err != nil {
				return nil, err
			}
			return wrapperspb.String("done"), nil
		},
	)
}

// backdate sets completed_at on a stored workflow record so the test can pretend
// the record is older than it really is. Real callers never need this — only
// tests that exercise retention thresholds.
func backdate(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string, completedAt time.Time) {
	t.Helper()
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
	record, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("expected stored record for %s/%s", workflowID, runID)
	}
	record.CompletedAt = timestamppb.New(completedAt)
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", record.GetStatus())
	}
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
}
