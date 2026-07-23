package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestAllActivitiesPreservesCallOrder(t *testing.T) {
	results, err := AllActivities(
		context.Background(),
		func(context.Context) (*wrapperspb.StringValue, error) {
			time.Sleep(20 * time.Millisecond)
			return wrapperspb.String("first"), nil
		},
		func(context.Context) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("second"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 ||
		results[0].GetValue() != "first" ||
		results[1].GetValue() != "second" {
		t.Fatalf("results = %v, want [first second]", results)
	}
}

func TestAllActivitiesWaitsForSlowSiblingBeforeWorkflowFails(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{
		WorkflowId: "fanout:settle",
		RunId:      "run",
	}
	slowStarted := make(chan struct{})
	fastFailed := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowFinished atomic.Bool

	done := make(chan error, 1)
	go func() {
		_, runErr := Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				_, fanoutErr := AllActivities(
					ctx,
					func(ctx context.Context) (*wrapperspb.StringValue, error) {
						return ExecuteActivity(
							ctx,
							&ActivityOptions{ActivityId: "slow"},
							wrapperspb.String("slow"),
							func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
							func(
								context.Context,
								*wrapperspb.StringValue,
							) (*wrapperspb.StringValue, error) {
								close(slowStarted)
								<-releaseSlow
								slowFinished.Store(true)
								return wrapperspb.String("slow:done"), nil
							},
						)
					},
					func(ctx context.Context) (*wrapperspb.StringValue, error) {
						return ExecuteActivity(
							ctx,
							&ActivityOptions{ActivityId: "fast-failure"},
							wrapperspb.String("fast"),
							func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
							func(
								context.Context,
								*wrapperspb.StringValue,
							) (*wrapperspb.StringValue, error) {
								<-slowStarted
								close(fastFailed)
								return nil, NewActivityError("fatal", "fast branch failed", nil)
							},
						)
					},
				)
				if fanoutErr != nil {
					return nil, fanoutErr
				}
				return wrapperspb.String("unreachable"), nil
			},
		)
		done <- runErr
	}()

	select {
	case <-fastFailed:
	case <-time.After(5 * time.Second):
		t.Fatal("fast activity did not fail")
	}
	select {
	case err := <-done:
		t.Fatalf("workflow returned before its slow sibling settled: %v", err)
	default:
	}
	if slowFinished.Load() {
		t.Fatal("slow sibling finished before it was released")
	}

	close(releaseSlow)
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not return after slow sibling settled")
	}
	var activityErr *ActivityError
	if !errors.As(runErr, &activityErr) || activityErr.Code != "fatal" {
		t.Fatalf("workflow error = %v, want fatal ActivityError", runErr)
	}
	if !slowFinished.Load() {
		t.Fatal("workflow returned before slow activity finished")
	}

	tests := []struct {
		name       string
		activityID string
		wantStatus temporalessv1.ActivityStatus
	}{
		{
			name:       "slow sibling completed",
			activityID: "slow",
			wantStatus: temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		},
		{
			name:       "fast sibling failed",
			activityID: "fast-failure",
			wantStatus: temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record, found, getErr := store.GetActivity(
				ctx,
				storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), test.activityID),
			)
			if getErr != nil || !found {
				t.Fatalf("activity record: err=%v found=%v", getErr, found)
			}
			if record.GetStatus() != test.wantStatus {
				t.Fatalf("activity status = %s, want %s", record.GetStatus(), test.wantStatus)
			}
		})
	}
}

func TestAllActivitiesContinuationWinsAndLeavesWorkflowInProgress(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{
		WorkflowId: "fanout:pending",
		RunId:      "run",
	}
	pending := &TimerPendingError{
		TimerID: "retry:pending",
		WakeAt:  time.Now().UTC().Add(time.Hour),
	}

	_, err := Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			_, fanoutErr := AllActivities(
				ctx,
				func(context.Context) (*wrapperspb.StringValue, error) {
					return nil, NewActivityError("permanent", "terminal branch", nil)
				},
				func(context.Context) (*wrapperspb.StringValue, error) {
					return nil, pending
				},
			)
			return nil, fanoutErr
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("workflow error = %v, want ErrTimerPending", err)
	}

	record, found, getErr := store.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow record: err=%v found=%v", getErr, found)
	}
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("workflow status = %s, want IN_PROGRESS", record.GetStatus())
	}
}

func TestAllActivitiesAggregatesTerminalFailures(t *testing.T) {
	first := errors.New("first failed")
	second := errors.New("second failed")
	_, err := AllActivities(
		context.Background(),
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, first
		},
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, second
		},
	)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("joined error = %v, want both terminal failures", err)
	}
}

func TestAllActivitiesCancellationDrainsChildrenAndKeepsWorkflowInProgress(t *testing.T) {
	store := newTestStore(t)
	options := &Options{
		WorkflowId: "fanout:cancel",
		RunId:      "run",
	}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	var drained atomic.Int32
	done := make(chan error, 1)

	go func() {
		_, runErr := Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				calls := []ActivityCall[*wrapperspb.StringValue]{
					func(ctx context.Context) (*wrapperspb.StringValue, error) {
						started <- struct{}{}
						<-ctx.Done()
						drained.Add(1)
						return nil, ctx.Err()
					},
					func(ctx context.Context) (*wrapperspb.StringValue, error) {
						started <- struct{}{}
						<-ctx.Done()
						drained.Add(1)
						return nil, ctx.Err()
					},
				}
				_, fanoutErr := AllActivities(ctx, calls...)
				return nil, fanoutErr
			},
		)
		done <- runErr
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("fan-out child did not start")
		}
	}
	cancel()

	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not return after cancellation")
	}
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("workflow error = %v, want context.Canceled", runErr)
	}
	if drained.Load() != 2 {
		t.Fatalf("drained children = %d, want 2", drained.Load())
	}
	record, found, getErr := store.GetWorkflow(
		context.Background(),
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow record: err=%v found=%v", getErr, found)
	}
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("workflow status = %s, want IN_PROGRESS", record.GetStatus())
	}
}

func TestAllActivitiesValidatesCallsBeforeStarting(t *testing.T) {
	called := false
	valid := func(context.Context) (*wrapperspb.StringValue, error) {
		called = true
		return wrapperspb.String("called"), nil
	}
	var missing ActivityCall[*wrapperspb.StringValue]

	_, err := AllActivities(context.Background(), valid, missing)
	if err == nil {
		t.Fatal("expected nil-call validation error")
	}
	if called {
		t.Fatal("valid call started before all calls were validated")
	}

	empty, err := AllActivities[*wrapperspb.StringValue](context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("empty results = %v, want empty", empty)
	}
}
