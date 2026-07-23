package workflow

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func forceSleepTimerDue(
	t *testing.T,
	ctx context.Context,
	store storage.Store,
	workflowID string,
	runID string,
	timerID string,
) storage.TimerKey {
	t.Helper()
	key := storage.NewTimerKey(workflowID, runID, timerID)
	record, found, err := store.GetTimer(ctx, key)
	if err != nil || !found {
		t.Fatalf("sleep timer: err=%v found=%v", err, found)
	}
	record.FireAt = timestamppb.New(time.Now().UTC().Add(-time.Second))
	record.Status = temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
	record.FiredAt = nil
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}
	return key
}

func requireTimerStatus(
	t *testing.T,
	ctx context.Context,
	store storage.Store,
	key storage.TimerKey,
	want temporalessv1.TimerStatus,
) {
	t.Helper()
	record, found, err := store.GetTimer(ctx, key)
	if err != nil || !found {
		t.Fatalf("timer %q: err=%v found=%v", key.TimerID, err, found)
	}
	if got := record.GetStatus(); got != want {
		t.Fatalf("timer %q status = %s, want %s", key.TimerID, got, want)
	}
}

func TestSleepDueTimerSurvivesCrashAfterActivityRecord(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "sleep-crash", RunId: "run"}
	crashAfterActivity := false
	activityCalls := 0
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "wake", time.Hour); err != nil {
			return nil, err
		}
		result, err := ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "after-sleep"},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls++
				return wrapperspb.String("activity-done"), nil
			},
		)
		if err != nil {
			return nil, err
		}
		if crashAfterActivity {
			panic("simulated process crash")
		}
		return result, nil
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want timer pending", err)
	}
	timerKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "wake")

	crashAfterActivity = true
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = Run(
			ctx, store, options, nil, wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
		)
	}()
	if recovered == nil {
		t.Fatal("expected simulated process crash")
	}
	if activityCalls != 1 {
		t.Fatalf("activity calls after crash = %d, want 1", activityCalls)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	workflowRecord, found, err := store.GetWorkflow(ctx, storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()))
	if err != nil || !found {
		t.Fatalf("workflow after crash: err=%v found=%v", err, found)
	}
	if got := workflowRecord.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("workflow after crash = %s, want IN_PROGRESS", got)
	}
	due, err := store.DueTimers(ctx, "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Key != timerKey {
		t.Fatalf("due timers after crash = %+v, want wake timer", due)
	}

	crashAfterActivity = false
	result, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "activity-done" {
		t.Fatalf("result = %q, want activity-done", got)
	}
	if activityCalls != 1 {
		t.Fatalf("activity calls after replay = %d, want 1", activityCalls)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
}

func TestSleepLaterScheduledTimerAcknowledgesConsumedTimer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "sleep-successor", RunId: "run"}
	body := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "first", time.Hour); err != nil {
			return nil, err
		}
		if err := Sleep(ctx, "second", 2*time.Hour); err != nil {
			return nil, err
		}
		return wrapperspb.String("done"), nil
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}
	firstKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "first")
	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("successor run error = %v, want pending on second timer", err)
	}
	requireTimerStatus(t, ctx, store, firstKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
	secondKey := storage.NewTimerKey(options.GetWorkflowId(), options.GetRunId(), "second")
	requireTimerStatus(t, ctx, store, secondKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
}

func TestSleepDurableActivityRetryTimerAcknowledgesConsumedTimer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "sleep-activity-retry-successor", RunId: "run"}
	policy := &RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      1,
		MaximumAttempts:         2,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "wake", time.Hour); err != nil {
			return nil, err
		}
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "retrying", RetryPolicy: policy, RetryTimerId: testRetryTimerID("retrying")},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return nil, NewActivityError("transient", "try again", nil)
			},
		)
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want sleep pending", err)
	}
	sleepKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "wake")
	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("activity retry run error = %v, want retry timer pending", err)
	}
	requireTimerStatus(t, ctx, store, sleepKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
	retryKey := storage.NewTimerKey(
		options.GetWorkflowId(),
		options.GetRunId(),
		testRetryTimerID("retrying"),
	)
	requireTimerStatus(t, ctx, store, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
}

type failTerminalWorkflowStore struct {
	storage.Store
	fail bool
	err  error
}

func (s *failTerminalWorkflowStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	if s.fail && (record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED ||
		record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED) {
		return s.err
	}
	return s.Store.PutWorkflow(ctx, record)
}

func TestSleepTerminalWriteFailureLeavesWakeupScheduled(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("terminal workflow write failed")
	store := &failTerminalWorkflowStore{Store: base, err: writeErr}
	options := &Options{WorkflowId: "sleep-terminal-write", RunId: "run"}
	bodyCalls := 0
	body := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "wake", time.Hour); err != nil {
			return nil, err
		}
		bodyCalls++
		return wrapperspb.String("done"), nil
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}
	timerKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "wake")
	store.fail = true
	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, writeErr) {
		t.Fatalf("terminal write error = %v, want injected error", err)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	due, err := store.DueTimers(ctx, "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Key != timerKey {
		t.Fatalf("due timers after failed terminal write = %+v, want wake timer", due)
	}

	store.fail = false
	result, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	if bodyCalls != 2 {
		t.Fatalf("post-sleep body calls = %d, want 2 (at-least-once after ambiguous terminal write)", bodyCalls)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
}

type boundaryOrderStore struct {
	storage.Store
	events []string
}

func (s *boundaryOrderStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	if err := s.Store.PutWorkflow(ctx, record); err != nil {
		return err
	}
	if record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		s.events = append(s.events, "workflow-failed")
	}
	return nil
}

func (s *boundaryOrderStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if err := s.Store.PutTimer(ctx, record); err != nil {
		return err
	}
	if record.GetTimerKind() == storage.SleepTimerKind &&
		record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		s.events = append(s.events, "timer-fired")
	}
	return nil
}

func TestSleepBodyErrorPersistsTerminalFailureBeforeAcknowledgement(t *testing.T) {
	ctx := context.Background()
	store := &boundaryOrderStore{Store: newTestStore(t)}
	options := &Options{WorkflowId: "sleep-body-error", RunId: "run"}
	bodyErr := errors.New("body failed after wake")
	body := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "wake", time.Hour); err != nil {
			return nil, err
		}
		return nil, bodyErr
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}
	timerKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "wake")
	store.events = nil
	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, bodyErr) {
		t.Fatalf("body error = %v, want injected error", err)
	}
	if len(store.events) != 2 || store.events[0] != "workflow-failed" || store.events[1] != "timer-fired" {
		t.Fatalf("durable boundary order = %v, want [workflow-failed timer-fired]", store.events)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
}

func TestSleepActivityClaimBusyLeavesWakeupScheduled(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claims := newTestClaimStore(t)
	options := &Options{
		WorkflowId:   "sleep-claim-busy",
		RunId:        "run",
		ClaimOwnerId: "runner",
	}
	activityCalls := 0
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		if err := Sleep(ctx, "wake", time.Hour); err != nil {
			return nil, err
		}
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "after-sleep"},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls++
				return wrapperspb.String("done"), nil
			},
		)
	}

	_, err := Run(
		ctx, store, options, claims, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}
	timerKey := forceSleepTimerDue(t, ctx, store, options.GetWorkflowId(), options.GetRunId(), "wake")
	claimKey := storage.NewClaimKey(
		options.GetWorkflowId(),
		options.GetRunId(),
		ActivityClaimIDPrefix+"after-sleep",
	)
	now := time.Now().UTC()
	created, err := claims.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion:  storage.ClaimRecordSchemaVersion,
		Key:            claimKey.Proto(),
		OwnerId:        "other-worker",
		ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
		ResourceId:     "after-sleep",
		LeaseExpiresAt: timestamppb.New(now.Add(time.Hour)),
		CreatedAt:      timestamppb.New(now),
		HeartbeatAt:    timestamppb.New(now),
	})
	if err != nil || !created {
		t.Fatalf("seed activity claim: err=%v created=%v", err, created)
	}

	_, err = Run(
		ctx, store, options, claims, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrClaimBusy) {
		t.Fatalf("claim-busy run error = %v, want claim busy", err)
	}
	if activityCalls != 0 {
		t.Fatalf("activity calls while claim busy = %d, want 0", activityCalls)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	due, err := store.DueTimers(ctx, "", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 1 || due[0].Key != timerKey {
		t.Fatalf("due timers while activity claim busy = %+v, want wake timer", due)
	}
	if _, err := claims.DeleteClaim(ctx, claimKey); err != nil {
		t.Fatal(err)
	}

	result, err := Run(
		ctx, store, options, claims, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	if activityCalls != 1 {
		t.Fatalf("activity calls after claim release = %d, want 1", activityCalls)
	}
	requireTimerStatus(t, ctx, store, timerKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
}

type ambiguousSleepWriteStore struct {
	storage.Store
	err          error
	commitBefore bool
	failed       bool
}

func (s *ambiguousSleepWriteStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if !s.failed &&
		record.GetTimerKind() == storage.SleepTimerKind &&
		record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		s.failed = true
		if s.commitBefore {
			if err := s.Store.PutTimer(ctx, record); err != nil {
				return err
			}
		}
		return s.err
	}
	return s.Store.PutTimer(ctx, record)
}

func TestSleepAmbiguousWriteIsVerifiedAndRemainsResumable(t *testing.T) {
	tests := []struct {
		name          string
		commitBefore  bool
		wantPending   bool
		wantFirstWake bool
	}{
		{name: "before commit", commitBefore: false, wantPending: false, wantFirstWake: false},
		{name: "after commit", commitBefore: true, wantPending: true, wantFirstWake: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := newTestStore(t)
			writeErr := errors.New("ambiguous sleep timer write")
			store := &ambiguousSleepWriteStore{
				Store:        base,
				err:          writeErr,
				commitBefore: test.commitBefore,
			}
			options := &Options{
				WorkflowId: "sleep-ambiguous-" + strings.ReplaceAll(test.name, " ", "-"),
				RunId:      "run",
			}
			body := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				if err := Sleep(ctx, "wake", time.Hour); err != nil {
					return nil, err
				}
				return wrapperspb.String("done"), nil
			}

			_, err := Run(
				ctx, store, options, nil, wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
			)
			if !errors.Is(err, ErrWorkflowInfrastructure) || !errors.Is(err, writeErr) {
				t.Fatalf("first run error = %v, want infrastructure and backend errors", err)
			}
			if got := errors.Is(err, ErrTimerPending); got != test.wantPending {
				t.Fatalf("first run timer pending = %v, want %v", got, test.wantPending)
			}

			workflowRecord, found, getErr := base.GetWorkflow(
				ctx,
				storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
			)
			if getErr != nil || !found {
				t.Fatalf("workflow after ambiguous write: found=%v err=%v", found, getErr)
			}
			if got := workflowRecord.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
				t.Fatalf("workflow status = %s, want IN_PROGRESS", got)
			}
			timerKey := storage.NewTimerKey(options.GetWorkflowId(), options.GetRunId(), "wake")
			_, firstWakeFound, getErr := base.GetTimer(ctx, timerKey)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if firstWakeFound != test.wantFirstWake {
				t.Fatalf("wake found after first run = %v, want %v", firstWakeFound, test.wantFirstWake)
			}

			// A before-commit failure is resumed by the requester's retry; an
			// after-commit failure replays the verified durable timer. Both paths
			// converge on the same scheduler-visible wake.
			_, err = Run(
				ctx, store, options, nil, wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
			)
			if !errors.Is(err, ErrTimerPending) {
				t.Fatalf("second run error = %v, want timer pending", err)
			}
			forceSleepTimerDue(
				t, ctx, base, options.GetWorkflowId(), options.GetRunId(), timerKey.TimerID,
			)
			due, dueErr := base.DueTimers(ctx, "", time.Now().UTC())
			if dueErr != nil {
				t.Fatal(dueErr)
			}
			if len(due) != 1 || due[0].Key != timerKey {
				t.Fatalf("due timers = %+v, want verified wake", due)
			}
		})
	}
}

type primitiveReadFailureStore struct {
	storage.Store
	err               error
	failActivity      bool
	failActivityWrite bool
	failEvent         bool
}

func (s *primitiveReadFailureStore) PutActivity(
	ctx context.Context,
	record *temporalessv1.ActivityRecord,
) error {
	if s.failActivityWrite {
		return s.err
	}
	return s.Store.PutActivity(ctx, record)
}

func (s *primitiveReadFailureStore) GetActivity(
	ctx context.Context,
	key storage.ActivityKey,
) (*temporalessv1.ActivityRecord, bool, error) {
	if s.failActivity {
		return nil, false, s.err
	}
	return s.Store.GetActivity(ctx, key)
}

func (s *primitiveReadFailureStore) GetEvent(
	ctx context.Context,
	key storage.EventKey,
) (*temporalessv1.EventRecord, bool, error) {
	if s.failEvent {
		return nil, false, s.err
	}
	return s.Store.GetEvent(ctx, key)
}

func TestWorkflowPrimitiveStorageFailuresRemainInProgress(t *testing.T) {
	tests := []struct {
		name              string
		failActivity      bool
		failActivityWrite bool
		failEvent         bool
		body              WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]
	}{
		{
			name:         "activity read",
			failActivity: true,
			body: func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return ExecuteActivity(
					ctx,
					&ActivityOptions{ActivityId: "fetch"},
					input,
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return wrapperspb.String("must-not-run"), nil
					},
				)
			},
		},
		{
			name:              "activity write",
			failActivityWrite: true,
			body: func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return ExecuteActivity(
					ctx,
					&ActivityOptions{ActivityId: "fetch"},
					input,
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return wrapperspb.String("completed-before-write-failed"), nil
					},
				)
			},
		},
		{
			name:      "event read",
			failEvent: true,
			body: func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return WaitEvent(ctx, "approval", func() *wrapperspb.StringValue {
					return &wrapperspb.StringValue{}
				}, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			base := newTestStore(t)
			backendErr := errors.New("record store unavailable")
			store := &primitiveReadFailureStore{
				Store:             base,
				err:               backendErr,
				failActivity:      test.failActivity,
				failActivityWrite: test.failActivityWrite,
				failEvent:         test.failEvent,
			}
			options := &Options{
				WorkflowId: "primitive-storage-" + strings.ReplaceAll(test.name, " ", "-"),
				RunId:      "run",
			}

			_, err := Run(
				ctx, store, options, nil, wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, test.body,
			)
			if !errors.Is(err, ErrWorkflowInfrastructure) || !errors.Is(err, backendErr) {
				t.Fatalf("run error = %v, want infrastructure and backend errors", err)
			}
			record, found, getErr := base.GetWorkflow(
				ctx,
				storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
			)
			if getErr != nil || !found {
				t.Fatalf("workflow: found=%v err=%v", found, getErr)
			}
			if got := record.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
				t.Fatalf("workflow status = %s, want IN_PROGRESS", got)
			}
		})
	}
}

func TestActivityBusinessErrorCannotMasqueradeAsWorkflowPending(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{WorkflowId: "activity-business-pending", RunId: "run"}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return ExecuteActivity(
				ctx,
				&ActivityOptions{ActivityId: "business"},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return nil, &TimerPendingError{TimerID: "user-value", WakeAt: time.Now().Add(time.Hour)}
				},
			)
		},
	)
	var activityErr *ActivityError
	if !errors.As(err, &activityErr) {
		t.Fatalf("run error = %v, want ActivityError", err)
	}
	record, found, getErr := store.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow: found=%v err=%v", found, getErr)
	}
	if got := record.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("workflow status = %s, want FAILED", got)
	}
}
