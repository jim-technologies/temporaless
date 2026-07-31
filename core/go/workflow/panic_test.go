package workflow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRunPersistsAndReplaysUserPanic(t *testing.T) {
	tests := []struct {
		name       string
		panicValue any
		wantValue  string
	}{
		{name: "string", panicValue: "workflow exploded", wantValue: "workflow exploded"},
		{name: "error", panicValue: errors.New("workflow error"), wantValue: "workflow error"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			options := &Options{WorkflowId: "panic:workflow:" + test.name, RunId: "run"}
			var calls atomic.Int32
			body := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				calls.Add(1)
				panic(test.panicValue)
			}

			_, err := Run(
				ctx,
				store,
				options,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				body,
			)
			requireDurableUserPanic(t, err, "workflow", test.wantValue)

			record, found, getErr := store.GetWorkflow(
				ctx,
				storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
			)
			if getErr != nil || !found {
				t.Fatalf("workflow record: err=%v found=%v", getErr, found)
			}
			if got := record.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
				t.Fatalf("workflow status = %s, want FAILED", got)
			}
			if got := record.GetFailure().GetCode(); got != temporalessv1.Default_ReservedNames_UserPanicErrorCode {
				t.Fatalf("failure code = %q, want user panic code", got)
			}

			_, replayErr := Run(
				ctx,
				store,
				options,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				body,
			)
			requireDurableUserPanic(t, replayErr, "", test.wantValue)
			if got := calls.Load(); got != 1 {
				t.Fatalf("workflow calls = %d, want 1 after replay", got)
			}
		})
	}
}

func TestWorkflowResultConstructorPanicIsContainedBeforeRunCreation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "panic:workflow-constructor", RunId: "run"}
	bodyCalled := false

	_, err := Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { panic("workflow constructor exploded") },
		func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			bodyCalled = true
			return wrapperspb.String("unexpected"), nil
		},
	)
	requireUserPanic(t, err, "workflow result constructor", "workflow constructor exploded")
	var activityErr *ActivityError
	if errors.As(err, &activityErr) {
		t.Fatalf("pre-record constructor error = %T %v, want bare UserPanicError", err, err)
	}
	if bodyCalled {
		t.Fatal("workflow body ran after result-constructor panic")
	}
	if _, found, getErr := store.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	); getErr != nil || found {
		t.Fatalf("workflow record after constructor panic: err=%v found=%v, want absent", getErr, found)
	}
}

func TestActivityResultConstructorPanicFailsWorkflowWithoutRunningActivity(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "panic:activity-constructor", RunId: "run"}
	activityCalled := false

	_, err := Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return ExecuteActivity(
				ctx,
				&ActivityOptions{ActivityId: "constructor"},
				input,
				func() *wrapperspb.StringValue { panic("activity constructor exploded") },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					activityCalled = true
					return wrapperspb.String("unexpected"), nil
				},
			)
		},
	)
	requireUserPanic(t, err, "activity result constructor", "activity constructor exploded")
	if activityCalled {
		t.Fatal("activity body ran after result-constructor panic")
	}
	if _, found, getErr := store.GetActivity(
		ctx,
		storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "constructor"),
	); getErr != nil || found {
		t.Fatalf("activity record after constructor panic: err=%v found=%v, want absent", getErr, found)
	}
	record, found, getErr := store.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow record: err=%v found=%v", getErr, found)
	}
	if got := record.GetFailure().GetCode(); got != temporalessv1.Default_ReservedNames_UserPanicErrorCode {
		t.Fatalf("workflow failure code = %q, want user panic code", got)
	}
}

func TestActivityPanicIsDurableAndNeverRetried(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "panic:activity", RunId: "run"}
	var activityCalls atomic.Int32
	retryPolicy := &RetryPolicy{
		InitialInterval:    durationpb.New(time.Nanosecond),
		BackoffCoefficient: 1,
		MaximumAttempts:    3,
	}

	execute := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "panic", RetryPolicy: retryPolicy},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls.Add(1)
				panic("activity exploded")
			},
		)
	}
	run := func() error {
		_, err := Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			execute,
		)
		return err
	}

	err := run()
	requireDurableUserPanic(t, err, "activity", "activity exploded")
	if got := activityCalls.Load(); got != 1 {
		t.Fatalf("activity calls = %d, want 1 non-retryable attempt", got)
	}

	replayErr := run()
	requireDurableUserPanic(t, replayErr, "", "activity exploded")
	if got := activityCalls.Load(); got != 1 {
		t.Fatalf("activity calls = %d, want 1 after replay", got)
	}

	record, found, getErr := store.GetActivity(
		ctx,
		storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "panic"),
	)
	if getErr != nil || !found {
		t.Fatalf("activity record: err=%v found=%v", getErr, found)
	}
	if got := record.GetStatus(); got != temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED {
		t.Fatalf("activity status = %s, want FAILED", got)
	}
	if got := record.GetFailure().GetCode(); got != temporalessv1.Default_ReservedNames_UserPanicErrorCode {
		t.Fatalf("failure code = %q, want user panic code", got)
	}
	if got := len(record.GetAttempts()); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestReservedUserPanicCodeIsTerminalAndStable(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "panic:reserved-code", RunId: "run"}
	var activityCalls atomic.Int32
	retryPolicy := &RetryPolicy{
		InitialInterval:    durationpb.New(time.Nanosecond),
		BackoffCoefficient: 1,
		MaximumAttempts:    3,
	}
	execute := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "reserved", RetryPolicy: retryPolicy},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls.Add(1)
				return nil, NewActivityError(
					temporalessv1.Default_ReservedNames_UserPanicErrorCode,
					"user supplied the reserved marker",
					errors.New("ordinary user error"),
				)
			},
		)
	}
	run := func() error {
		_, err := Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			execute,
		)
		return err
	}

	firstErr := run()
	requireDurableUserPanic(t, firstErr, "", "user supplied the reserved marker")
	replayErr := run()
	requireDurableUserPanic(t, replayErr, "", "user supplied the reserved marker")
	if got := activityCalls.Load(); got != 1 {
		t.Fatalf("activity calls = %d, want one terminal attempt after replay", got)
	}

	record, found, getErr := store.GetActivity(
		ctx,
		storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "reserved"),
	)
	if getErr != nil || !found {
		t.Fatalf("activity record: err=%v found=%v", getErr, found)
	}
	if got := record.GetStatus(); got != temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED {
		t.Fatalf("activity status = %s, want FAILED", got)
	}
	if got := len(record.GetAttempts()); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestAllActivitiesPanicDrainsSiblingsAndPersistsWorkflowFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "panic:fanout", RunId: "run"}
	slowStarted := make(chan struct{})
	panicRaised := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowSettled atomic.Bool
	var workflowCalls atomic.Int32
	done := make(chan error, 1)
	execute := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		workflowCalls.Add(1)
		_, fanoutErr := AllActivities(
			ctx,
			func(context.Context) (*wrapperspb.StringValue, error) {
				close(slowStarted)
				<-releaseSlow
				slowSettled.Store(true)
				return wrapperspb.String("settled"), nil
			},
			func(context.Context) (*wrapperspb.StringValue, error) {
				<-slowStarted
				close(panicRaised)
				panic("fanout exploded")
			},
		)
		return nil, fanoutErr
	}

	go func() {
		_, runErr := Run(
			ctx,
			store,
			options,
			nil,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			execute,
		)
		done <- runErr
	}()

	select {
	case <-panicRaised:
	case <-time.After(5 * time.Second):
		t.Fatal("fan-out branch did not panic")
	}
	select {
	case err := <-done:
		t.Fatalf("workflow returned before sibling settled: %v", err)
	default:
	}

	close(releaseSlow)
	var runErr error
	select {
	case runErr = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not return after sibling settled")
	}
	requireDurableUserPanic(t, runErr, "AllActivities branch", "fanout exploded")
	if !slowSettled.Load() {
		t.Fatal("fan-out returned before slow sibling settled")
	}

	record, found, getErr := store.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow record: err=%v found=%v", getErr, found)
	}
	if got := record.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("workflow status = %s, want FAILED", got)
	}
	if got := record.GetFailure().GetCode(); got != temporalessv1.Default_ReservedNames_UserPanicErrorCode {
		t.Fatalf("failure code = %q, want user panic code", got)
	}

	_, replayErr := Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	requireDurableUserPanic(t, replayErr, "", "fanout exploded")
	if got := workflowCalls.Load(); got != 1 {
		t.Fatalf("workflow calls = %d, want 1 after replay", got)
	}
}

func requireUserPanic(t *testing.T, err error, wantBoundary string, wantValue string) {
	t.Helper()
	if !errors.Is(err, ErrUserPanic) {
		t.Fatalf("error = %T %v, want ErrUserPanic", err, err)
	}
	var panicErr *UserPanicError
	if !errors.As(err, &panicErr) {
		t.Fatalf("error = %T %v, want UserPanicError", err, err)
	}
	if wantBoundary != "" && panicErr.Boundary != wantBoundary {
		t.Fatalf("panic boundary = %q, want %q", panicErr.Boundary, wantBoundary)
	}
	if panicErr.PanicValue != "" && panicErr.PanicValue != wantValue {
		t.Fatalf("panic value = %q, want %q", panicErr.PanicValue, wantValue)
	}
	if !strings.Contains(panicErr.Error(), wantValue) {
		t.Fatalf("panic error = %q, want value %q", panicErr.Error(), wantValue)
	}
}

func requireDurableUserPanic(t *testing.T, err error, wantBoundary string, wantValue string) {
	t.Helper()
	requireUserPanic(t, err, wantBoundary, wantValue)
	var activityErr *ActivityError
	if !errors.As(err, &activityErr) {
		t.Fatalf("durable panic error = %T %v, want top-level ActivityError", err, err)
	}
	if reflect.TypeOf(err) != reflect.TypeOf(activityErr) {
		t.Fatalf("durable panic error = %T %v, want ActivityError without an outer wrapper", err, err)
	}
	if got := activityErr.Code; got != temporalessv1.Default_ReservedNames_UserPanicErrorCode {
		t.Fatalf("durable panic code = %q, want %q", got, temporalessv1.Default_ReservedNames_UserPanicErrorCode)
	}
	if !strings.Contains(activityErr.Message, wantValue) {
		t.Fatalf("durable panic message = %q, want value %q", activityErr.Message, wantValue)
	}
	var panicCause *UserPanicError
	if !errors.As(activityErr.Cause, &panicCause) {
		t.Fatalf("durable panic cause = %T %v, want direct UserPanicError", activityErr.Cause, activityErr.Cause)
	}
	if reflect.TypeOf(activityErr.Cause) != reflect.TypeOf(panicCause) {
		t.Fatalf("durable panic cause = %T %v, want UserPanicError without an inner wrapper", activityErr.Cause, activityErr.Cause)
	}
}
