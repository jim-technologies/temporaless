package inspector_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/inspector"
	"github.com/jim-technologies/temporaless/adapters/go/scanquery"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestListActivitiesAndResetHelpers(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	options := &workflow.Options{WorkflowId: "prices:reset", RunId: "2026-05-04", CodeVersion: "test-version"}
	calls := 0
	wfBody := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		out, err := workflow.ExecuteActivity(
			ctx,
			&workflow.ActivityOptions{ActivityId: "fetch:" + input.GetValue()},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				calls++
				return wrapperspb.String("ok:" + request.GetValue()), nil
			},
		)
		return out, err
	}

	if _, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		wfBody,
	); err != nil {
		t.Fatal(err)
	}

	wfKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:reset",
		RunID:      "2026-05-04",
	}
	activities, err := inspector.ListActivities(ctx, store, wfKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(activities); got != 1 {
		t.Fatalf("activity count = %d, want 1", got)
	}
	if activities[0].GetKey().GetActivityId() != "fetch:AAPL" {
		t.Fatalf("activity id = %q", activities[0].GetKey().GetActivityId())
	}

	// Reset the activity record only — replay should re-execute the activity
	// (workflow record is still cached, so the activity would normally replay too).
	// To force re-execution end-to-end we also reset the workflow record.
	if err := inspector.ResetWorkflow(ctx, store, wfKey); err != nil {
		t.Fatal(err)
	}
	if err := inspector.ResetActivity(ctx, store, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:reset",
		RunID:      "2026-05-04",
		ActivityID: "fetch:AAPL",
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		wfBody,
	); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("activity calls after reset = %d, want 2", calls)
	}
}

func TestResetEventForcesEventPendingAgain(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	eventKey := storage.EventKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:event-reset",
		RunID:      "2026-05-04",
		EventID:    "approval",
	}
	if err := storage.SendEvent(ctx, store, eventKey, wrapperspb.String("manager")); err != nil {
		t.Fatal(err)
	}

	if _, found, _ := store.GetEvent(ctx, eventKey); !found {
		t.Fatal("expected event delivered")
	}

	if err := inspector.ResetEvent(ctx, store, eventKey); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.GetEvent(ctx, eventKey); found {
		t.Fatal("expected event cleared after reset")
	}
}

func TestResetIsIdempotentOnMissingPath(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	missingKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "missing",
		RunID:      "missing",
	}
	if err := inspector.ResetWorkflow(ctx, store, missingKey); err != nil {
		t.Fatalf("expected reset on missing record to be a no-op, got %v", err)
	}
}

func TestListInFlightAndFailedWorkflows(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Run 1: completes.
	if _, err := workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: "prices:done", RunId: "2026-05-04", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("ok"), nil
		},
	); err != nil {
		t.Fatal(err)
	}

	// Run 2: leaves IN_PROGRESS via a timer pending.
	_, runErr := workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: "prices:waiting", RunId: "2026-05-04", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := workflow.Sleep(ctx, "wait", time.Hour); err != nil {
				return nil, err
			}
			return wrapperspb.String("ok"), nil
		},
	)
	if !errors.Is(runErr, workflow.ErrTimerPending) {
		t.Fatalf("expected ErrTimerPending, got %v", runErr)
	}

	// Run 3: fails.
	_, failErr := workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: "prices:broken", RunId: "2026-05-04", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, workflow.NewActivityError("upstream_5xx", "boom", nil)
		},
	)
	if failErr == nil {
		t.Fatal("expected failure")
	}

	inFlight, err := inspector.ListInFlightWorkflows(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(inFlight); got != 1 {
		t.Fatalf("in-flight count = %d, want 1", got)
	}
	if inFlight[0].GetKey().GetWorkflowId() != "prices:waiting" {
		t.Fatalf("in-flight workflow = %q", inFlight[0].GetKey().GetWorkflowId())
	}

	failed, err := inspector.ListFailedWorkflows(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(failed); got != 1 {
		t.Fatalf("failed count = %d, want 1", got)
	}
	if failed[0].GetKey().GetWorkflowId() != "prices:broken" {
		t.Fatalf("failed workflow = %q", failed[0].GetKey().GetWorkflowId())
	}
	if failed[0].GetFailure().GetCode() != "upstream_5xx" {
		t.Fatalf("failure code = %q", failed[0].GetFailure().GetCode())
	}

	completed, err := inspector.ListWorkflowsByStatus(
		ctx,
		query,
		temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(completed); got != 1 {
		t.Fatalf("completed count = %d, want 1", got)
	}
	if completed[0].GetKey().GetWorkflowId() != "prices:done" {
		t.Fatalf("completed workflow = %q", completed[0].GetKey().GetWorkflowId())
	}
}
