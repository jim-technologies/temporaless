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

// D2: durable retry backoffs. When RetryPolicy.DurableBackoffThreshold > 0 and
// the next retry interval crosses it, the runtime persists the wait as a
// TIMER_KIND_ACTIVITY_RETRY timer + writes the ActivityRecord with
// next_attempt_at, then surfaces TimerPendingError so the workflow stays
// IN_PROGRESS and a downstream scanner re-invokes after fire_at.

func testRetryTimerID(activityID string) string {
	return "retry=" + activityID
}

type cancelAfterRetryingStore struct {
	storage.Store
	cancel context.CancelFunc
}

func (store *cancelAfterRetryingStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	if err := store.Store.PutActivity(ctx, record); err != nil {
		return err
	}
	if record.GetStatus() == temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		store.cancel()
	}
	return nil
}

func TestDurableRetry_ShortBackoffStaysInProcess(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Millisecond),
		BackoffCoefficient:      1.0,
		MaximumInterval:         durationpb.New(10 * time.Millisecond),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(1 * time.Hour), // way above interval
	}

	attemptCount := 0
	_, err := runActivity(
		ctx, wf,
		"act:short",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:short"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			attemptCount++
			if attemptCount < 3 {
				return nil, NewActivityError("flaky", "transient", nil)
			}
			return wrapperspb.String("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if attemptCount != 3 {
		t.Fatalf("attempts = %d, want 3 (all in-process)", attemptCount)
	}

	// No retry timer should have been written.
	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    testRetryTimerID("act:short"),
	}
	_, found, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no retry timer for in-process backoff")
	}
}

func TestDurableRetry_ShortBackoffPersistsTimerIDAndResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	baseStore := newTestStore(t)
	store := &cancelAfterRetryingStore{Store: baseStore, cancel: cancel}
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "short-resume",
	}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Second),
		BackoffCoefficient:      1,
		MaximumAttempts:         2,
		DurableBackoffThreshold: durationpb.New(time.Hour),
	}
	retryTimerID := testRetryTimerID("act:short-resume")
	attempts := 0
	execute := func(context.Context) (*wrapperspb.StringValue, error) {
		attempts++
		if attempts == 1 {
			return nil, NewActivityError("flaky", "transient", nil)
		}
		return wrapperspb.String("ok"), nil
	}

	_, err := runActivity(
		ctx,
		wf,
		"act:short-resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		retryTimerID,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first call error = %v, want context cancellation after RETRYING persistence", err)
	}

	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act:short-resume")
	retrying, found, err := baseStore.GetActivity(context.Background(), activityKey)
	if err != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", err, found)
	}
	if retrying.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		t.Fatalf("status = %v, want RETRYING", retrying.GetStatus())
	}
	if got := retrying.GetRetryTimerId(); got != retryTimerID {
		t.Fatalf("RETRYING retry_timer_id = %q, want %q", got, retryTimerID)
	}
	if retrying.GetNextAttemptAt() != nil {
		t.Fatal("short-backoff RETRYING record unexpectedly has next_attempt_at")
	}
	if _, timerFound, timerErr := baseStore.GetTimer(
		context.Background(),
		storage.NewTimerKey(wf.workflowID, wf.runID, retryTimerID),
	); timerErr != nil || timerFound {
		t.Fatalf("short-backoff retry timer: err=%v found=%v, want absent", timerErr, timerFound)
	}

	result, err := runActivity(
		context.Background(),
		wf,
		"act:short-resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		retryTimerID,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "ok" || attempts != 2 {
		t.Fatalf("result = %q, attempts = %d, want ok after 2", result.GetValue(), attempts)
	}
	completed, found, err := baseStore.GetActivity(context.Background(), activityKey)
	if err != nil || !found {
		t.Fatalf("completed activity: err=%v found=%v", err, found)
	}
	if completed.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", completed.GetStatus())
	}
	if got := completed.GetRetryTimerId(); got != retryTimerID {
		t.Fatalf("terminal retry_timer_id = %q, want %q", got, retryTimerID)
	}
}

func TestDurableRetry_LongBackoffPersistsAndBails(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(30 * time.Minute),
		BackoffCoefficient:      1.0,
		MaximumInterval:         durationpb.New(30 * time.Minute),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}

	attemptCount := 0
	start := time.Now().UTC()
	_, err := runActivity(
		ctx, wf,
		"act:long",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:long"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			attemptCount++
			return nil, NewActivityError("rate_limited", "vendor 429", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("err = %v, want TimerPendingError", err)
	}
	if attemptCount != 1 {
		t.Fatalf("attempts = %d, want 1 (durable bail after first failure)", attemptCount)
	}
	var tp *TimerPendingError
	if !errors.As(err, &tp) {
		t.Fatalf("err type = %T", err)
	}
	if tp.TimerID != testRetryTimerID("act:long") {
		t.Fatalf("timer id = %q, want caller-supplied ID", tp.TimerID)
	}
	if tp.WakeAt.Before(start.Add(29 * time.Minute)) {
		t.Fatalf("wake_at = %s, want ~30 minutes from start", tp.WakeAt)
	}

	// ActivityRecord must be RETRYING with next_attempt_at set.
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "act:long",
	}
	record, found, err := store.GetActivity(ctx, key)
	if err != nil || !found {
		t.Fatalf("err=%v found=%v", err, found)
	}
	if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		t.Fatalf("status = %s", record.GetStatus())
	}
	if record.GetNextAttemptAt() == nil {
		t.Fatal("next_attempt_at not set")
	}
	if len(record.GetAttempts()) != 1 {
		t.Fatalf("attempts = %d", len(record.GetAttempts()))
	}

	// Paired timer must be SCHEDULED with the same fire_at.
	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    testRetryTimerID("act:long"),
	}
	timer, timerFound, err := store.GetTimer(ctx, timerKey)
	if err != nil || !timerFound {
		t.Fatalf("retry timer: err=%v found=%v", err, timerFound)
	}
	if timer.GetTimerKind() != temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
		t.Fatalf("kind = %s", timer.GetTimerKind())
	}
	if got := timer.GetRetryActivityId(); got != "act:long" {
		t.Fatalf("retry_activity_id = %q, want act:long", got)
	}
	if timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("timer status = %s", timer.GetStatus())
	}
	if !timer.GetFireAt().AsTime().Equal(record.GetNextAttemptAt().AsTime()) {
		t.Fatalf("timer fire_at != activity next_attempt_at: %s vs %s",
			timer.GetFireAt().AsTime(), record.GetNextAttemptAt().AsTime())
	}
}

func TestDurableRetry_ReplayBeforeFireAtReturnsPending(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}
	// Seed a RETRYING record with next_attempt_at in the future. Activity type
	// must match what runActivity will compute below.
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Minute),
		BackoffCoefficient:      1.0,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}
	future := time.Now().UTC().Add(10 * time.Minute)
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "act:wait",
	}
	if err := store.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           key.Proto(),
		ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		NextAttemptAt: timestamppb.New(future),
		CreatedAt:     timestamppb.Now(),
		Failure:       &temporalessv1.ActivityFailure{Code: "retryable", Message: "try again"},
		RetryPolicy:   policy,
		RetryTimerId:  testRetryTimerID("act:wait"),
		Attempts: []*temporalessv1.ActivityAttempt{
			{
				Attempt:     1,
				StartedAt:   timestamppb.Now(),
				CompletedAt: timestamppb.Now(),
				Failure:     &temporalessv1.ActivityFailure{Code: "retryable", Message: "try again"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	_, err := runActivity(
		ctx, wf,
		"act:wait",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:wait"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("ok"), nil
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("err = %v, want pending", err)
	}
	if executions != 0 {
		t.Fatalf("executions = %d, want 0 (replay must not re-run before wake)", executions)
	}
}

func TestDurableRetry_ReplayAfterFireAtResumes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}

	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Minute),
		BackoffCoefficient:      1.0,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}
	past := time.Now().UTC().Add(-1 * time.Minute)
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "act:resume",
	}
	if err := store.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           key.Proto(),
		ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		NextAttemptAt: timestamppb.New(past),
		CreatedAt:     timestamppb.Now(),
		Failure:       &temporalessv1.ActivityFailure{Code: "retryable", Message: "try again"},
		RetryPolicy:   policy,
		RetryTimerId:  testRetryTimerID("act:resume"),
		Attempts: []*temporalessv1.ActivityAttempt{
			{
				Attempt:     1,
				StartedAt:   timestamppb.Now(),
				CompletedAt: timestamppb.Now(),
				Failure:     &temporalessv1.ActivityFailure{Code: "retryable", Message: "try again"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// Seed the paired timer so we can verify it gets marked FIRED.
	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    testRetryTimerID("act:resume"),
	}
	if err := store.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion:   storage.TimerRecordSchemaVersion,
		Key:             timerKey.Proto(),
		TimerKind:       temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
		Duration:        durationpb.New(time.Minute),
		Status:          temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:          timestamppb.New(past),
		CreatedAt:       timestamppb.Now(),
		RetryActivityId: "act:resume",
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	result, err := runActivity(
		ctx, wf,
		"act:resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:resume"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "ok" {
		t.Fatalf("result = %q", result.GetValue())
	}
	if executions != 1 {
		t.Fatalf("executions = %d, want 1 (resume runs attempt 2)", executions)
	}
	// Activity must be COMPLETED with attempt history preserved.
	record, found, err := store.GetActivity(ctx, key)
	if err != nil || !found {
		t.Fatalf("err=%v found=%v", err, found)
	}
	if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("status = %s", record.GetStatus())
	}
	if len(record.GetAttempts()) != 2 {
		t.Fatalf("attempts = %d, want 2 (preserved attempt 1 + new attempt 2)", len(record.GetAttempts()))
	}
	// Paired timer must now be FIRED.
	timer, _, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		t.Fatalf("timer status = %s, want FIRED", timer.GetStatus())
	}
}

func TestDurableRetry_SecondLongBackoffOverwritesTimer(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Minute),
		BackoffCoefficient:      2.0,
		MaximumInterval:         durationpb.New(40 * time.Minute),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}

	// First attempt — fails, durable retry written.
	attemptCount := 0
	failOnce := func(_ context.Context) (*wrapperspb.StringValue, error) {
		attemptCount++
		return nil, NewActivityError("rate_limited", "vendor 429", nil)
	}
	_, err := runActivity(
		ctx, wf, "act:multi",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy, testRetryTimerID("act:multi"), wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		failOnce,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("first call: err = %v", err)
	}

	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    testRetryTimerID("act:multi"),
	}
	t1, _, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if got := t1.GetDuration().AsDuration(); got != 10*time.Minute {
		t.Fatalf("first timer duration = %s, want exactly 10m", got)
	}
	firstFireAt := t1.GetFireAt().AsTime()

	// Force-rewind the stored activity's next_attempt_at into the past so the
	// next call resumes immediately, then artificially fail again to trigger
	// the second durable backoff (which the planner will compute as ~20min).
	activityKey := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "act:multi",
	}
	record, _, err := store.GetActivity(ctx, activityKey)
	if err != nil {
		t.Fatal(err)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	record.NextAttemptAt = timestamppb.New(dueAt)
	if err := store.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	t1.FireAt = timestamppb.New(dueAt)
	if err := store.PutTimer(ctx, t1); err != nil {
		t.Fatal(err)
	}

	_, err = runActivity(
		ctx, wf, "act:multi",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy, testRetryTimerID("act:multi"), wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		failOnce,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("second call: err = %v", err)
	}

	t2, _, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if t2.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("second timer status = %s, want SCHEDULED", t2.GetStatus())
	}
	if got := t2.GetDuration().AsDuration(); got != 20*time.Minute {
		t.Fatalf("second timer duration = %s, want exactly 20m", got)
	}
	if !t2.GetFireAt().AsTime().After(firstFireAt) {
		t.Fatalf("second timer fire_at = %s should be later than first %s", t2.GetFireAt().AsTime(), firstFireAt)
	}
}

func TestDurableRetry_PriorRetryAfterDoesNotCompoundNextPolicyDelay(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "retry-after-resume",
	}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(10 * time.Minute),
		BackoffCoefficient:      2.0,
		MaximumInterval:         durationpb.New(4 * time.Hour),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}

	attempts := 0
	execute := func(_ context.Context) (*wrapperspb.StringValue, error) {
		attempts++
		if attempts == 1 {
			return nil, NewRetryableActivityError("rate_limited", "vendor 429", time.Hour, nil)
		}
		return nil, NewActivityError("transient", "try again", nil)
	}
	_, err := runActivity(
		ctx, wf, "act:retry-after-resume", activityClaimTestType, policy,
		testRetryTimerID("act:retry-after-resume"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("first call: error = %v, want pending", err)
	}

	timerKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act:retry-after-resume"))
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("first timer: err=%v found=%v", err, found)
	}
	if got := timer.GetDuration().AsDuration(); got != time.Hour {
		t.Fatalf("first timer duration = %s, want Retry-After 1h", got)
	}

	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act:retry-after-resume")
	record, found, err := store.GetActivity(ctx, activityKey)
	if err != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", err, found)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	record.NextAttemptAt = timestamppb.New(dueAt)
	if err := store.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	timer.FireAt = timestamppb.New(dueAt)
	if err := store.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

	_, err = runActivity(
		ctx, wf, "act:retry-after-resume", activityClaimTestType, policy,
		testRetryTimerID("act:retry-after-resume"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("second call: error = %v, want pending", err)
	}
	timer, found, err = store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("second timer: err=%v found=%v", err, found)
	}
	if got := timer.GetDuration().AsDuration(); got != 20*time.Minute {
		t.Fatalf("second timer duration = %s, want policy delay 20m (not compounded from 1h Retry-After)", got)
	}
}

func TestRetryIntervalAfterAttemptCapsMaximum(t *testing.T) {
	plan, err := planRetries(&temporalessv1.RetryPolicy{
		InitialInterval:    durationpb.New(10 * time.Minute),
		BackoffCoefficient: 2.0,
		MaximumInterval:    durationpb.New(20 * time.Minute),
		MaximumAttempts:    5,
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		attempt uint32
		want    time.Duration
	}{
		{attempt: 1, want: 10 * time.Minute},
		{attempt: 2, want: 20 * time.Minute},
		{attempt: 3, want: 20 * time.Minute},
		{attempt: 4, want: 20 * time.Minute},
	}
	for _, test := range tests {
		got, err := retryIntervalAfterAttempt(test.attempt, plan)
		if err != nil {
			t.Fatalf("attempt %d: %v", test.attempt, err)
		}
		if got != test.want {
			t.Fatalf("attempt %d interval = %s, want %s", test.attempt, got, test.want)
		}
	}
}

func TestRetryingActivityRejectsNegativePersistedRetryAfter(t *testing.T) {
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:    durationpb.New(time.Minute),
		BackoffCoefficient: 2,
		MaximumAttempts:    3,
	}
	plan, err := planRetries(policy)
	if err != nil {
		t.Fatal(err)
	}
	record := &temporalessv1.ActivityRecord{
		Status:      temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		RetryPolicy: normalizeRetryPolicy(policy, plan),
		Attempts: []*temporalessv1.ActivityAttempt{
			{
				Attempt: 1,
				Failure: &temporalessv1.ActivityFailure{
					Code:       "rate_limited",
					RetryAfter: durationpb.New(-time.Second),
				},
			},
		},
	}
	if err := assertRetryingActivity(record, normalizeRetryPolicy(policy, plan), "", plan); !errors.Is(err, ErrActivityConflict) {
		t.Fatalf("error = %v, want activity conflict", err)
	}
}

func TestDurableRetry_RetryingPolicyChangeConflictsBeforeExecution(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "policy-change"}
	original := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      2,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	_, err := runActivity(
		ctx, wf, "act:policy", activityClaimTestType, original,
		testRetryTimerID("act:policy"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial call: error = %v, want pending", err)
	}

	changed := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      3,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	executions := 0
	_, err = runActivity(
		ctx, wf, "act:policy", activityClaimTestType, changed,
		testRetryTimerID("act:policy"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrActivityConflict) {
		t.Fatalf("changed policy error = %v, want activity conflict", err)
	}
	if executions != 0 {
		t.Fatalf("executions = %d, want 0", executions)
	}
}

func TestDurableRetry_RetryTimerIDChangeConflictsBeforeExecution(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{store: store, workflowID: "wf", runID: "timer-id-change"}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      1,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	_, err := runActivity(
		ctx, wf, "act:id", activityClaimTestType, policy, "retry:original",
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial call: error = %v, want pending", err)
	}
	record, found, err := store.GetActivity(ctx, storage.NewActivityKey(wf.workflowID, wf.runID, "act:id"))
	if err != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", err, found)
	}
	if got := record.GetRetryTimerId(); got != "retry:original" {
		t.Fatalf("stored retry_timer_id = %q, want retry:original", got)
	}

	executions := 0
	_, err = runActivity(
		ctx, wf, "act:id", activityClaimTestType, policy, "retry:changed",
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrActivityConflict) {
		t.Fatalf("changed timer ID error = %v, want activity conflict", err)
	}
	if executions != 0 {
		t.Fatalf("executions = %d, want 0", executions)
	}
	if _, found, getErr := store.GetTimer(ctx, storage.NewTimerKey(wf.workflowID, wf.runID, "retry:changed")); getErr != nil || found {
		t.Fatalf("changed timer ID was written: err=%v found=%v", getErr, found)
	}
}

func TestDurableRetry_CrashDuringDueAttemptLeavesWakeupScheduled(t *testing.T) {
	tests := []struct {
		name       string
		withClaims bool
	}{
		{name: "without claims"},
		{name: "with create-only activity claim", withClaims: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			wf := &Workflow{
				store:      store,
				workflowID: "wf",
				runID:      "crash-" + strings.ReplaceAll(test.name, " ", "-"),
			}
			if test.withClaims {
				wf.claimStore = newTestClaimStore(t)
				wf.claimOwner = "worker-1"
			}
			policy := &temporalessv1.RetryPolicy{
				InitialInterval:         durationpb.New(time.Hour),
				BackoffCoefficient:      1,
				MaximumAttempts:         3,
				DurableBackoffThreshold: durationpb.New(time.Second),
			}

			_, err := runActivity(
				ctx, wf, "act:crash", activityClaimTestType, policy,
				testRetryTimerID("act:crash"),
				wrapperspb.String("x"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context) (*wrapperspb.StringValue, error) {
					return nil, NewActivityError("transient", "try again", nil)
				},
			)
			if !errors.Is(err, ErrTimerPending) {
				t.Fatalf("initial call: error = %v, want pending", err)
			}

			activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act:crash")
			record, found, err := store.GetActivity(ctx, activityKey)
			if err != nil || !found {
				t.Fatalf("retrying activity: err=%v found=%v", err, found)
			}
			dueAt := time.Now().UTC().Add(-time.Second)
			record.NextAttemptAt = timestamppb.New(dueAt)
			if err := store.PutActivity(ctx, record); err != nil {
				t.Fatal(err)
			}
			timerKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act:crash"))
			timer, found, err := store.GetTimer(ctx, timerKey)
			if err != nil || !found {
				t.Fatalf("retry timer before crash: err=%v found=%v", err, found)
			}
			timer.FireAt = timestamppb.New(dueAt)
			if err := store.PutTimer(ctx, timer); err != nil {
				t.Fatal(err)
			}

			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_, _ = runActivity(
					ctx, wf, "act:crash", activityClaimTestType, policy,
					testRetryTimerID("act:crash"),
					wrapperspb.String("x"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context) (*wrapperspb.StringValue, error) {
						panic("simulated process crash")
					},
				)
			}()
			if recovered == nil {
				t.Fatal("expected simulated process crash")
			}

			timer, found, err = store.GetTimer(ctx, timerKey)
			if err != nil || !found {
				t.Fatalf("retry timer: err=%v found=%v", err, found)
			}
			if got := timer.GetStatus(); got != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
				t.Fatalf("timer after crash = %s, want SCHEDULED", got)
			}

			successCalls := 0
			succeed := func(context.Context) (*wrapperspb.StringValue, error) {
				successCalls++
				return wrapperspb.String("recovered"), nil
			}
			if test.withClaims {
				_, err = runActivity(
					ctx, wf, "act:crash", activityClaimTestType, policy,
					testRetryTimerID("act:crash"),
					wrapperspb.String("x"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					succeed,
				)
				if !errors.Is(err, ErrClaimBusy) {
					t.Fatalf("retry with crash-retained claim = %v, want claim busy", err)
				}
				if successCalls != 0 {
					t.Fatalf("success calls while claim busy = %d, want 0", successCalls)
				}
				if _, err := wf.claimStore.DeleteClaim(ctx, activityClaimKeyForTest(wf, "act:crash")); err != nil {
					t.Fatal(err)
				}
			}

			result, err := runActivity(
				ctx, wf, "act:crash", activityClaimTestType, policy,
				testRetryTimerID("act:crash"),
				wrapperspb.String("x"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				succeed,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := result.GetValue(); got != "recovered" {
				t.Fatalf("result = %q, want recovered", got)
			}
			timer, found, err = store.GetTimer(ctx, timerKey)
			if err != nil || !found {
				t.Fatalf("final retry timer: err=%v found=%v", err, found)
			}
			if got := timer.GetStatus(); got != temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
				t.Fatalf("timer after terminal activity = %s, want FIRED", got)
			}
		})
	}
}

type retryTimerCleanupFailStore struct {
	storage.Store
	failCleanup bool
	err         error
}

func (s *retryTimerCleanupFailStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if s.failCleanup &&
		record.GetTimerKind() == temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY &&
		record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		return s.err
	}
	return s.Store.PutTimer(ctx, record)
}

func TestDurableRetry_TimerCleanupFailurePreservesTerminalResultAndReplayRepairs(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	cleanupErr := errors.New("timer cleanup write failed")
	store := &retryTimerCleanupFailStore{Store: base, err: cleanupErr}
	wf := &Workflow{store: store, workflowID: "wf", runID: "cleanup-failure"}
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      1,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}

	_, err := runActivity(
		ctx, wf, "act:cleanup", activityClaimTestType, policy,
		testRetryTimerID("act:cleanup"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("transient", "try again", nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial call: error = %v, want pending", err)
	}

	activityKey := storage.NewActivityKey(wf.workflowID, wf.runID, "act:cleanup")
	record, found, err := base.GetActivity(ctx, activityKey)
	if err != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", err, found)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	record.NextAttemptAt = timestamppb.New(dueAt)
	if err := base.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	timerKey := storage.NewTimerKey(wf.workflowID, wf.runID, testRetryTimerID("act:cleanup"))
	timer, found, err := base.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("retry timer before cleanup: err=%v found=%v", err, found)
	}
	timer.FireAt = timestamppb.New(dueAt)
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

	store.failCleanup = true
	executions := 0
	result, err := runActivity(
		ctx, wf, "act:cleanup", activityClaimTestType, policy,
		testRetryTimerID("act:cleanup"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("done"), nil
		},
	)
	if err != nil {
		t.Fatalf("terminal result was replaced by cleanup error: %v", err)
	}
	if got := result.GetValue(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	record, found, err = base.GetActivity(ctx, activityKey)
	if err != nil || !found {
		t.Fatalf("terminal activity: err=%v found=%v", err, found)
	}
	if got := record.GetStatus(); got != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("activity status = %s, want COMPLETED before timer cleanup", got)
	}
	timer, found, err = base.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("scheduled timer: err=%v found=%v", err, found)
	}
	if got := timer.GetStatus(); got != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("timer status after failed cleanup = %s, want SCHEDULED", got)
	}

	store.failCleanup = false
	result, err = runActivity(
		ctx, wf, "act:cleanup", activityClaimTestType, policy,
		testRetryTimerID("act:cleanup"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.GetValue(); got != "done" {
		t.Fatalf("replayed result = %q, want done", got)
	}
	if executions != 1 {
		t.Fatalf("activity executions = %d, want 1", executions)
	}
	timer, found, err = base.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("repaired timer: err=%v found=%v", err, found)
	}
	if got := timer.GetStatus(); got != temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		t.Fatalf("timer after terminal replay = %s, want FIRED", got)
	}
}

func TestDurableRetry_TimerCleanupFailureDoesNotFailCompletedWorkflow(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	store := &retryTimerCleanupFailStore{
		Store: base,
		err:   errors.New("timer cleanup write failed"),
	}
	options := &Options{WorkflowId: "retry-cleanup-workflow", RunId: "run"}
	policy := &RetryPolicy{
		InitialInterval:         durationpb.New(time.Hour),
		BackoffCoefficient:      1,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Second),
	}
	activityCalls := 0
	body := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "call", RetryPolicy: policy, RetryTimerId: testRetryTimerID("call")},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				activityCalls++
				if activityCalls == 1 {
					return nil, NewActivityError("transient", "try again", nil)
				}
				return wrapperspb.String("done"), nil
			},
		)
	}

	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}
	activityKey := storage.NewActivityKey(options.GetWorkflowId(), options.GetRunId(), "call")
	record, found, err := base.GetActivity(ctx, activityKey)
	if err != nil || !found {
		t.Fatalf("retrying activity: err=%v found=%v", err, found)
	}
	dueAt := time.Now().UTC().Add(-time.Second)
	record.NextAttemptAt = timestamppb.New(dueAt)
	if err := base.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}
	timerKey := storage.NewTimerKey(options.GetWorkflowId(), options.GetRunId(), testRetryTimerID("call"))
	timer, found, err := base.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("retry timer before completion: err=%v found=%v", err, found)
	}
	timer.FireAt = timestamppb.New(dueAt)
	if err := base.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}

	store.failCleanup = true
	result, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, body,
	)
	if err != nil {
		t.Fatalf("workflow result was replaced by activity timer cleanup error: %v", err)
	}
	if got := result.GetValue(); got != "done" {
		t.Fatalf("result = %q, want done", got)
	}
	workflowRecord, found, err := base.GetWorkflow(ctx, storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()))
	if err != nil || !found {
		t.Fatalf("workflow record: err=%v found=%v", err, found)
	}
	if got := workflowRecord.GetStatus(); got != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("workflow status = %s, want COMPLETED", got)
	}
	timer, found, err = base.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("retry timer: err=%v found=%v", err, found)
	}
	if got := timer.GetStatus(); got != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		t.Fatalf("timer status after failed cleanup = %s, want SCHEDULED", got)
	}
	due, err := base.DueTimers(ctx, "", time.Now().UTC().Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(due) != 0 {
		t.Fatalf("terminal workflow exposed stale retry timer as due: %+v", due)
	}
}

func TestSleepAllowsCallerChosenTimerIDWithoutFrameworkPrefixReservation(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:      store,
		workflowID: "wf",
		runID:      "r",
	}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	err := Sleep(ctx, "activity-retry:foo", time.Minute)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("error = %v, want timer pending", err)
	}
	record, found, getErr := store.GetTimer(ctx, storage.NewTimerKey(wf.workflowID, wf.runID, "activity-retry:foo"))
	if getErr != nil || !found {
		t.Fatalf("sleep timer: err=%v found=%v", getErr, found)
	}
	if got := record.GetTimerKind(); got != temporalessv1.TimerKind_TIMER_KIND_SLEEP {
		t.Fatalf("timer kind = %s, want SLEEP", got)
	}
}
