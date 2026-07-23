package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type retryTimerWriteFailureMode uint8

const (
	retryTimerWriteBeforeCommit retryTimerWriteFailureMode = iota + 1
	retryTimerWriteAfterCommit
)

type retryTimerWriteFaultStore struct {
	storage.Store

	mu       sync.Mutex
	writes   int
	failures map[int]retryTimerWriteFailureMode
	err      error
}

type retryActivityWriteFaultStore struct {
	storage.Store

	mu       sync.Mutex
	writes   int
	failures map[int]retryTimerWriteFailureMode
	err      error
}

type cancelAfterRetryTimerStore struct {
	storage.Store
	cancel context.CancelFunc
	once   sync.Once
}

func (s *cancelAfterRetryTimerStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if err := s.Store.PutTimer(ctx, record); err != nil {
		return err
	}
	if record.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY &&
		record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		s.once.Do(s.cancel)
	}
	return nil
}

func (s *cancelAfterRetryTimerStore) PutActivity(
	ctx context.Context,
	record *temporalessv1.ActivityRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.Store.PutActivity(ctx, record)
}

func (s *retryActivityWriteFaultStore) PutActivity(
	ctx context.Context,
	record *temporalessv1.ActivityRecord,
) error {
	if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		return s.Store.PutActivity(ctx, record)
	}
	s.mu.Lock()
	s.writes++
	mode := s.failures[s.writes]
	s.mu.Unlock()
	switch mode {
	case retryTimerWriteBeforeCommit:
		return s.err
	case retryTimerWriteAfterCommit:
		if err := s.Store.PutActivity(ctx, record); err != nil {
			return err
		}
		return s.err
	default:
		return s.Store.PutActivity(ctx, record)
	}
}

func (s *retryTimerWriteFaultStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if record.GetTimerKind() != temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY ||
		record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		return s.Store.PutTimer(ctx, record)
	}

	s.mu.Lock()
	s.writes++
	mode := s.failures[s.writes]
	s.mu.Unlock()
	switch mode {
	case retryTimerWriteBeforeCommit:
		return s.err
	case retryTimerWriteAfterCommit:
		if err := s.Store.PutTimer(ctx, record); err != nil {
			return err
		}
		return s.err
	default:
		return s.Store.PutTimer(ctx, record)
	}
}

func durableRepairPolicy() *temporalessv1.RetryPolicy {
	return &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      1,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
}

func TestActivityRetryTimer_FirstWriteFailureRepairsBeforeAcknowledgingSleep(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("first retry timer write failed")
	store := &retryTimerWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{1: retryTimerWriteBeforeCommit},
		err:      writeErr,
	}
	options := &Options{WorkflowId: "retry-timer-first-write", RunId: "run"}
	policy := durableRepairPolicy()
	activityCalls := 0
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
				activityCalls++
				return nil, NewActivityError("transient", "try again", nil)
			},
		)
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial sleep run error = %v, want pending", err)
	}
	sleepKey := forceSleepTimerDue(t, ctx, base, options.GetWorkflowId(), options.GetRunId(), "wake")

	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, writeErr) {
		t.Fatalf("failed retry-timer boundary error = %v, want timer pending joined with write error", err)
	}
	if activityCalls != 1 {
		t.Fatalf("activity calls after failed boundary = %d, want 1", activityCalls)
	}
	requireTimerStatus(t, ctx, base, sleepKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	retryKey := storage.NewTimerKey(
		options.GetWorkflowId(),
		options.GetRunId(),
		testRetryTimerID("retrying"),
	)
	if _, found, getErr := base.GetTimer(ctx, retryKey); getErr != nil || found {
		t.Fatalf("retry timer after failed first write: err=%v found=%v, want absent", getErr, found)
	}

	_, err = Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("repair replay error = %v, want pending", err)
	}
	if errors.Is(err, writeErr) {
		t.Fatalf("repair replay retained obsolete write error: %v", err)
	}
	if activityCalls != 2 {
		t.Fatalf("activity calls after retrying an uncommitted attempt = %d, want 2", activityCalls)
	}
	requireTimerStatus(t, ctx, base, sleepKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
	requireTimerStatus(t, ctx, base, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)

	activityKey := storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "retrying")
	activity, found, getErr := base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", getErr, found)
	}
	timer, found, getErr := base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("repaired retry timer: err=%v found=%v", getErr, found)
	}
	if !timer.GetFireAt().AsTime().Equal(activity.GetNextAttemptAt().AsTime()) {
		t.Fatalf("repaired fire_at = %s, want next_attempt_at %s", timer.GetFireAt().AsTime(), activity.GetNextAttemptAt().AsTime())
	}
	if got := timer.GetDuration().AsDuration(); got != time.Hour {
		t.Fatalf("repaired timer duration = %s, want 1h", got)
	}
}

func TestActivityRetryTimer_AmbiguousWriteIsVerifiedByAuthoritativeRead(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("response lost after retry timer commit")
	store := &retryTimerWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{1: retryTimerWriteAfterCommit},
		err:      writeErr,
	}
	wf := &Workflow{store: store, workflowID: "wf", runID: "ambiguous-write"}
	executions := 0
	_, err := runActivity(
		ctx, wf, "act", activityClaimTestType, durableRepairPolicy(), testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("error = %v, want normal timer pending", err)
	}
	if errors.Is(err, writeErr) {
		t.Fatalf("verified committed write leaked ambiguous error: %v", err)
	}
	if executions != 1 {
		t.Fatalf("executions = %d, want 1", executions)
	}
	retryKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act"))
	requireTimerStatus(t, ctx, base, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
}

func TestActivityRetryTimer_LaterOverwriteFailureRepairsOnFutureReplay(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("second retry timer overwrite failed")
	store := &retryTimerWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{2: retryTimerWriteBeforeCommit},
		err:      writeErr,
	}
	wf := &Workflow{store: store, workflowID: "wf", runID: "later-overwrite"}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Minute),
		BackoffCoefficient:      2,
		MaximumAttempts:         4,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	executions := 0
	execute := func(context.Context) (*wrapperspb.StringValue, error) {
		executions++
		return nil, NewActivityError("transient", "try again", nil)
	}
	_, err := runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("first attempt error = %v, want pending", err)
	}

	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act")
	retryKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act"))
	activity, found, getErr := base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("first retrying activity: err=%v found=%v", getErr, found)
	}
	timer, found, getErr := base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("first retry timer: err=%v found=%v", getErr, found)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	activity.NextAttemptAt = timestamppb.New(dueAt)
	timer.FireAt = timestamppb.New(dueAt)
	if err := base.PutActivity(ctx, activity); err != nil {
		t.Fatal(err)
	}
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

	_, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, writeErr) {
		t.Fatalf("second retry boundary error = %v, want pending joined with overwrite error", err)
	}
	if executions != 2 {
		t.Fatalf("executions after failed overwrite = %d, want 2", executions)
	}
	_, found, getErr = base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("second retrying activity: err=%v found=%v", getErr, found)
	}
	timer, found, getErr = base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("stale retry timer: err=%v found=%v", getErr, found)
	}
	if !timer.GetFireAt().AsTime().Equal(dueAt) {
		t.Fatalf("timer was unexpectedly advanced after failed overwrite: %s", timer.GetFireAt().AsTime())
	}

	_, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("future repair replay error = %v, want pending", err)
	}
	if executions != 3 {
		t.Fatalf("executions after retrying the uncommitted second attempt = %d, want 3", executions)
	}
	activity, found, getErr = base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("repaired retrying activity: err=%v found=%v", getErr, found)
	}
	wantWakeAt := activity.GetNextAttemptAt().AsTime()
	timer, found, getErr = base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("advanced retry timer: err=%v found=%v", getErr, found)
	}
	if !timer.GetFireAt().AsTime().Equal(wantWakeAt) {
		t.Fatalf("advanced timer fire_at = %s, want %s", timer.GetFireAt().AsTime(), wantWakeAt)
	}
	if got := timer.GetDuration().AsDuration(); got != 20*time.Minute {
		t.Fatalf("advanced timer duration = %s, want 20m", got)
	}
}

func TestActivityRetryTimer_NewerPreparedWakeSurvivesLaterActivityWriteFailure(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("later RETRYING activity write failed")
	store := &retryActivityWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{2: retryTimerWriteBeforeCommit},
		err:      writeErr,
	}
	wf := &Workflow{store: store, workflowID: "wf", runID: "newer-prepare"}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Minute),
		BackoffCoefficient:      2,
		MaximumAttempts:         4,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	executions := 0
	execute := func(context.Context) (*wrapperspb.StringValue, error) {
		executions++
		if executions <= 2 {
			return nil, NewActivityError("transient", "try again", nil)
		}
		return wrapperspb.String("recovered"), nil
	}
	_, err := runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("first attempt error = %v, want pending", err)
	}

	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act")
	retryKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act"))
	activity, found, getErr := base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("first retrying activity: err=%v found=%v", getErr, found)
	}
	timer, found, getErr := base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("first retry timer: err=%v found=%v", getErr, found)
	}
	oldDueAt := time.Now().UTC().Add(-2 * time.Second)
	activity.NextAttemptAt = timestamppb.New(oldDueAt)
	timer.FireAt = timestamppb.New(oldDueAt)
	if err := base.PutActivity(ctx, activity); err != nil {
		t.Fatal(err)
	}
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

	_, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, writeErr) {
		t.Fatalf("later activity boundary error = %v, want pending joined with write error", err)
	}
	if executions != 2 {
		t.Fatalf("executions after failed activity write = %d, want 2", executions)
	}
	activity, found, getErr = base.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("lagging retrying activity: err=%v found=%v", getErr, found)
	}
	if len(activity.GetAttempts()) != 1 || !activity.GetNextAttemptAt().AsTime().Equal(oldDueAt) {
		t.Fatalf("lagging activity was unexpectedly advanced: attempts=%d next=%s", len(activity.GetAttempts()), activity.GetNextAttemptAt().AsTime())
	}
	timer, found, getErr = base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("newer prepared timer: err=%v found=%v", getErr, found)
	}
	preparedWakeAt := timer.GetFireAt().AsTime()
	if !preparedWakeAt.After(oldDueAt) || timer.GetDuration().AsDuration() != 20*time.Minute {
		t.Fatalf("prepared timer = fire_at %s duration %s, want newer 20m wake", preparedWakeAt, timer.GetDuration().AsDuration())
	}

	_, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("prepared replay error = %v, want pending", err)
	}
	if executions != 2 {
		t.Fatalf("newer prepared wake did not delay duplicate attempt: executions=%d", executions)
	}
	timer, found, getErr = base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("prepared timer after replay: err=%v found=%v", getErr, found)
	}
	if !timer.GetFireAt().AsTime().Equal(preparedWakeAt) {
		t.Fatalf("prepared timer regressed from %s to %s", preparedWakeAt, timer.GetFireAt().AsTime())
	}

	preparedDueAt := time.Now().UTC().Add(-100 * time.Millisecond)
	timer.FireAt = timestamppb.New(preparedDueAt)
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}
	result, err := runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, execute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "recovered" {
		t.Fatalf("result = %q, want recovered", got)
	}
	if executions != 3 {
		t.Fatalf("executions after prepared wake = %d, want 3", executions)
	}
}

func TestActivityRetryTimer_CancellationAfterPrepareStillReleasesActivityClaim(t *testing.T) {
	base := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	store := &cancelAfterRetryTimerStore{Store: base, cancel: cancel}
	claimStore := newTestClaimStore(t)
	wf := &Workflow{
		store:      store,
		claimStore: claimStore,
		workflowID: "wf",
		runID:      "cancel-after-prepare",
		claimOwner: "worker-1",
	}

	_, err := runActivity(
		ctx, wf, "act", activityClaimTestType, durableRepairPolicy(), testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want timer pending joined with cancellation", err)
	}
	if _, found, getErr := claimStore.GetClaim(
		context.Background(),
		activityClaimKeyForTest(wf, "act"),
	); getErr != nil || found {
		t.Fatalf("activity claim after canceled prepare boundary: err=%v found=%v, want released", getErr, found)
	}
	retryKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act"))
	requireTimerStatus(t, context.Background(), base, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	if _, found, getErr := base.GetActivity(
		context.Background(),
		storage.NewActivityKey(wf.workflowID, wf.runID, "act"),
	); getErr != nil || found {
		t.Fatalf("activity after canceled RETRYING write: err=%v found=%v, want absent", getErr, found)
	}
}

func TestActivityRetryTimer_PreparedWakeSurvivesActivityWriteFailureAndCrash(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("initial RETRYING activity write failed")
	store := &retryActivityWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{1: retryTimerWriteBeforeCommit},
		err:      writeErr,
	}
	options := &Options{WorkflowId: "retry-timer-due-crash", RunId: "run"}
	policy := durableRepairPolicy()
	activityCalls := 0
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "retrying", RetryPolicy: policy, RetryTimerId: testRetryTimerID("retrying")},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls++
				switch activityCalls {
				case 1:
					return nil, NewActivityError("transient", "try again", nil)
				case 2:
					panic("simulated process crash")
				default:
					return wrapperspb.String("recovered"), nil
				}
			},
		)
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, writeErr) {
		t.Fatalf("initial failed boundary error = %v, want pending joined with write error", err)
	}
	activityKey := storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "retrying")
	if _, found, getErr := base.GetActivity(ctx, activityKey); getErr != nil || found {
		t.Fatalf("activity after failed RETRYING write: err=%v found=%v, want absent", getErr, found)
	}
	retryKey := storage.NewTimerKey(
		options.GetWorkflowId(),
		options.GetRunId(),
		testRetryTimerID("retrying"),
	)
	timer, found, getErr := base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("prepared retry timer: err=%v found=%v", getErr, found)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	timer.FireAt = timestamppb.New(dueAt)
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

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
	if activityCalls != 2 {
		t.Fatalf("activity calls after crash = %d, want 2", activityCalls)
	}
	requireTimerStatus(t, ctx, base, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	timer, found, getErr = base.GetTimer(ctx, retryKey)
	if getErr != nil || !found {
		t.Fatalf("repaired due timer: err=%v found=%v", getErr, found)
	}
	if !timer.GetFireAt().AsTime().Equal(dueAt) {
		t.Fatalf("repaired due timer fire_at = %s, want %s", timer.GetFireAt().AsTime(), dueAt)
	}
	workflowRecord, found, getErr := base.GetWorkflow(
		ctx,
		storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()),
	)
	if getErr != nil || !found {
		t.Fatalf("workflow after crash: err=%v found=%v", getErr, found)
	}
	if got := workflowRecord.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("workflow after crash = %s, want IN_PROGRESS", got)
	}
	due, getErr := base.DueTimers(ctx, "", time.Now().UTC())
	if getErr != nil {
		t.Fatal(getErr)
	}
	if len(due) != 1 || due[0].Key != retryKey {
		t.Fatalf("due timers after crash = %+v, want retry wake", due)
	}

	result, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "recovered" {
		t.Fatalf("result = %q, want recovered", got)
	}
	if activityCalls != 3 {
		t.Fatalf("activity calls after redelivery = %d, want 3", activityCalls)
	}
	requireTimerStatus(t, ctx, base, retryKey, temporalessv1.TimerStatus_TIMER_STATUS_FIRED)
}

func TestActivityRetryTimer_IncompatibleReservedIDCollisionIsRejected(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	writeErr := errors.New("initial retry timer write failed")
	store := &retryTimerWriteFaultStore{
		Store:    base,
		failures: map[int]retryTimerWriteFailureMode{1: retryTimerWriteBeforeCommit},
		err:      writeErr,
	}
	wf := &Workflow{store: store, workflowID: "wf", runID: "collision"}
	policy := durableRepairPolicy()
	_, err := runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) || !errors.Is(err, writeErr) {
		t.Fatalf("initial failed boundary error = %v, want pending joined with write error", err)
	}

	retryKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act"))
	if err := base.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           retryKey.Proto(),
		TimerKind:     temporalessv1.TimerKind_TIMER_KIND_SLEEP,
		Duration:      durationpb.New(time.Hour),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(time.Now().UTC().Add(time.Hour)),
		CreatedAt:     timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	_, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, testRetryTimerID("act"), wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrTimerConflict) {
		t.Fatalf("collision error = %v, want timer conflict", err)
	}
	if errors.Is(err, ErrTimerPending) {
		t.Fatalf("collision was treated as repairable pending: %v", err)
	}
	if executions != 0 {
		t.Fatalf("executions after collision = %d, want 0", executions)
	}
}

func TestActivityRetryTimer_CallerIDCannotBeSharedByTwoActivities(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "shared-timer-id"}
	policy := durableRepairPolicy()
	const sharedTimerID = "retry:shared"
	_, err := runActivity(
		ctx, wf, "first", activityClaimTestType, policy, sharedTimerID, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("first activity error = %v, want pending", err)
	}
	timer, found, err := store.GetTimer(ctx, storage.NewTimerKey(wf.workflowID, wf.runID, sharedTimerID))
	if err != nil || !found {
		t.Fatalf("shared timer: err=%v found=%v", err, found)
	}
	if got := timer.GetRetryActivityId(); got != "first" {
		t.Fatalf("retry_activity_id = %q, want first", got)
	}

	secondCalls := 0
	_, err = runActivity(
		ctx, wf, "second", activityClaimTestType, policy, sharedTimerID, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			secondCalls++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrTimerConflict) {
		t.Fatalf("second activity error = %v, want timer conflict", err)
	}
	if secondCalls != 0 {
		t.Fatalf("second activity calls = %d, want 0", secondCalls)
	}
}

func TestActivityRetryTimer_TerminalCleanupRefusesIncompatibleTimer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "terminal-collision"}
	policy := durableRepairPolicy()
	timerID := testRetryTimerID("act")
	result, err := runActivity(
		ctx, wf, "act", activityClaimTestType, policy, timerID, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("stored"), nil
		},
	)
	if err != nil || result.GetValue() != "stored" {
		t.Fatalf("initial result=%v error=%v", result, err)
	}
	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act")
	record, found, err := store.GetActivity(ctx, activityKey)
	if err != nil || !found {
		t.Fatalf("terminal activity: err=%v found=%v", err, found)
	}
	record.RetryTimerId = timerID
	if err := store.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	timerKey := storage.NewTimerKey(wf.workflowID, wf.runID, timerID)
	if err := store.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           timerKey.Proto(),
		TimerKind:     temporalessv1.TimerKind_TIMER_KIND_SLEEP,
		Duration:      durationpb.New(time.Hour),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(time.Now().UTC().Add(time.Hour)),
		CreatedAt:     timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	replayCalls := 0
	result, err = runActivity(
		ctx, wf, "act", activityClaimTestType, policy, timerID, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			replayCalls++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if err != nil || result.GetValue() != "stored" {
		t.Fatalf("terminal replay result=%v error=%v", result, err)
	}
	if replayCalls != 0 {
		t.Fatalf("replay calls = %d, want 0", replayCalls)
	}
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("collision timer: err=%v found=%v", err, found)
	}
	if timer.GetTimerKind() != temporalessv1.TimerKind_TIMER_KIND_SLEEP ||
		timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("terminal cleanup mutated incompatible timer: kind=%s status=%s", timer.GetTimerKind(), timer.GetStatus())
	}
}
