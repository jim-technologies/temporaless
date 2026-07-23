package dependencies_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/dependencies"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type dependencyReadFailureStore struct {
	storage.Store
	err   error
	reads int
}

func (store *dependencyReadFailureStore) GetWorkflow(
	ctx context.Context,
	key storage.WorkflowKey,
) (*temporalessv1.WorkflowRecord, bool, error) {
	if key.WorkflowID == "upstream" {
		store.reads++
		return nil, false, store.err
	}
	return store.Store.GetWorkflow(ctx, key)
}

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
		&workflow.Options{WorkflowId: "upstream", RunId: runID},
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
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.GetValue() != "AAPL:100" {
		t.Fatalf("got %q, want %q", got.GetValue(), "AAPL:100")
	}
}

func TestWaitForWorkflowRejectsMismatchedResultType(t *testing.T) {
	store := newStore(t)
	seedCompleted(t, store, "type-mismatch", "AAPL:100")

	_, err := dependencies.WaitForWorkflow(
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "type-mismatch"),
		func() *wrapperspb.Int32Value { return &wrapperspb.Int32Value{} },
		nil,
	)
	if !errors.Is(err, workflow.ErrWorkflowConflict) {
		t.Fatalf("error = %v, want ErrWorkflowConflict", err)
	}
}

func TestWaitForWorkflowRejectsCorruptCompletedResult(t *testing.T) {
	tests := []struct {
		name   string
		runID  string
		mutate func(*temporalessv1.WorkflowRecord)
	}{
		{
			name:  "missing result",
			runID: "corrupt-missing",
			mutate: func(record *temporalessv1.WorkflowRecord) {
				record.Result = nil
			},
		},
		{
			name:  "malformed result",
			runID: "corrupt-malformed",
			mutate: func(record *temporalessv1.WorkflowRecord) {
				record.Result = &anypb.Any{
					TypeUrl: "type.googleapis.com/google.protobuf.StringValue",
					Value:   []byte{0xff},
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newStore(t)
			seedCompleted(t, store, test.runID, "AAPL:100")
			key := storage.NewWorkflowKey("upstream", test.runID)
			record, found, err := store.GetWorkflow(ctx, key)
			if err != nil || !found {
				t.Fatalf("workflow found=%v err=%v", found, err)
			}
			test.mutate(record)
			if err := store.PutWorkflow(ctx, record); err != nil {
				t.Fatal(err)
			}

			_, err = dependencies.WaitForWorkflow(
				ctx,
				store,
				key,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				nil,
			)
			if !errors.Is(err, storage.ErrCorruptRecord) {
				t.Fatalf("error = %v, want ErrCorruptRecord", err)
			}
		})
	}
}

func TestWaitForWorkflowReturnsPendingWhenUpstreamMissing(t *testing.T) {
	store := newStore(t)

	_, err := dependencies.WaitForWorkflow(
		context.Background(),
		store,
		storage.NewWorkflowKey("upstream", "missing"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		nil,
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
		&workflow.Options{WorkflowId: "upstream", RunId: "2026-05-04"},
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
		nil,
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
			nil,
		)
		if err != nil {
			return nil, err
		}
		return wrapperspb.String("signal(" + upstream.GetValue() + ")"), nil
	}

	first, err := workflow.Run(
		context.Background(),
		store,
		&workflow.Options{WorkflowId: "signal", RunId: "2026-05-04"},
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
		&workflow.Options{WorkflowId: "signal", RunId: "2026-05-04"},
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
		nil,
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
		nil,
	)
	if err == nil || err.Error() != "newResult is required" {
		t.Fatalf("expected newResult required error, got %v", err)
	}
}

func TestWaitForWorkflowPollSchedulesAndTerminallyAcknowledges(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	options := &workflow.Options{
		WorkflowId: "downstream",
		RunId:      "run",
	}
	poll := &workflow.PollOptions{
		TimerId:  "poll:upstream",
		Interval: durationpb.New(time.Hour),
	}
	body := func(
		ctx context.Context,
		_ *wrapperspb.StringValue,
	) (*wrapperspb.StringValue, error) {
		current, ok := workflow.Current(ctx)
		if !ok {
			return nil, errors.New("workflow context missing")
		}
		return dependencies.WaitForWorkflow(
			ctx,
			current.Store(),
			storage.NewWorkflowKey("upstream", "partition"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			poll,
		)
	}

	_, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	)
	var pending *workflow.WorkflowDependencyPendingError
	if !errors.As(err, &pending) || pending.WakeAt.IsZero() {
		t.Fatalf("first error = %#v, want polling dependency pending", err)
	}
	timerKey := storage.NewTimerKey(
		options.GetWorkflowId(),
		options.GetRunId(),
		poll.GetTimerId(),
	)
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found ||
		timer.GetTimerKind() != storage.PollTimerKind ||
		timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("poll timer found=%v err=%v timer=%v", found, err, timer)
	}

	seedCompleted(t, store, "partition", "ready")
	result, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	)
	if err != nil || result.GetValue() != "ready" {
		t.Fatalf("resolved result=%v err=%v", result, err)
	}
	timer, found, err = store.GetTimer(ctx, timerKey)
	if err != nil || !found ||
		timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		t.Fatalf("acknowledged timer found=%v err=%v timer=%v", found, err, timer)
	}
}

func TestWaitForWorkflowReadOutageLeavesParentInProgress(t *testing.T) {
	ctx := context.Background()
	base := newStore(t)
	backendErr := errors.New("dependency store unavailable")
	store := &dependencyReadFailureStore{Store: base, err: backendErr}
	options := &workflow.Options{
		WorkflowId: "downstream",
		RunId:      "outage",
	}
	body := func(
		ctx context.Context,
		_ *wrapperspb.StringValue,
	) (*wrapperspb.StringValue, error) {
		current, _ := workflow.Current(ctx)
		return dependencies.WaitForWorkflow(
			ctx,
			current.Store(),
			storage.NewWorkflowKey("upstream", "partition"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			nil,
		)
	}
	_, err := workflow.Run(
		ctx,
		store,
		options,
		nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		body,
	)
	if !errors.Is(err, workflow.ErrWorkflowInfrastructure) ||
		!errors.Is(err, backendErr) {
		t.Fatalf("run error=%v, want workflow infrastructure + backend errors", err)
	}
	record, found, getErr := base.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found ||
		record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("parent found=%v err=%v record=%v", found, getErr, record)
	}
}

func TestWaitForWorkflowValidatesKeyAndResultFactoryBeforeRead(t *testing.T) {
	ctx := context.Background()
	store := &dependencyReadFailureStore{
		Store: newStore(t),
		err:   errors.New("must not read"),
	}
	_, err := dependencies.WaitForWorkflow(
		ctx,
		store,
		storage.WorkflowKey{WorkflowID: ".", RunID: "run"},
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		nil,
	)
	if err == nil || store.reads != 0 {
		t.Fatalf("invalid key error=%v reads=%d, want fail-fast", err, store.reads)
	}

	var typedNil *wrapperspb.StringValue
	_, err = dependencies.WaitForWorkflow(
		ctx,
		store,
		storage.NewWorkflowKey("upstream", "run"),
		func() *wrapperspb.StringValue { return typedNil },
		nil,
	)
	if err == nil || err.Error() != "newResult returned nil" || store.reads != 0 {
		t.Fatalf("nil factory error=%v reads=%d", err, store.reads)
	}
}
