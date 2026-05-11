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

func TestDurableRetry_ShortBackoffStaysInProcess(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
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
		TimerID:    activityRetryTimerID("act:short"),
	}
	_, found, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected no retry timer for in-process backoff")
	}
}

func TestDurableRetry_LongBackoffPersistsAndBails(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
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
	if !strings.HasPrefix(tp.TimerID, ActivityRetryTimerIDPrefix) {
		t.Fatalf("timer id = %q", tp.TimerID)
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
		TimerID:    activityRetryTimerID("act:long"),
	}
	timer, timerFound, err := store.GetTimer(ctx, timerKey)
	if err != nil || !timerFound {
		t.Fatalf("retry timer: err=%v found=%v", err, timerFound)
	}
	if timer.GetTimerKind() != temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
		t.Fatalf("kind = %s", timer.GetTimerKind())
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
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}
	// Seed a RETRYING record with next_attempt_at in the future. Activity
	// type + digest must match what runActivity will compute below.
	digest, err := activityDigest(
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		wf.codeVersion, wrapperspb.String("x"),
	)
	if err != nil {
		t.Fatal(err)
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
		CodeVersion:   wf.codeVersion,
		InputDigest:   digest,
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		NextAttemptAt: timestamppb.New(future),
		CreatedAt:     timestamppb.Now(),
		Attempts: []*temporalessv1.ActivityAttempt{
			{Attempt: 1, StartedAt: timestamppb.Now(), CompletedAt: timestamppb.Now()},
		},
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	_, err = runActivity(
		ctx, wf,
		"act:wait",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		&temporalessv1.RetryPolicy{
			InitialInterval:         durationpb.New(10 * time.Minute),
			BackoffCoefficient:      1.0,
			MaximumAttempts:         3,
			DurableBackoffThreshold: durationpb.New(30 * time.Second),
		},
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
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}

	digest, err := activityDigest(
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		wf.codeVersion, wrapperspb.String("x"),
	)
	if err != nil {
		t.Fatal(err)
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
		CodeVersion:   wf.codeVersion,
		InputDigest:   digest,
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		NextAttemptAt: timestamppb.New(past),
		CreatedAt:     timestamppb.Now(),
		Attempts: []*temporalessv1.ActivityAttempt{
			{Attempt: 1, StartedAt: timestamppb.Now(), CompletedAt: timestamppb.Now()},
		},
	}); err != nil {
		t.Fatal(err)
	}
	// Seed the paired timer so we can verify it gets marked FIRED.
	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    activityRetryTimerID("act:resume"),
	}
	if err := store.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           timerKey.Proto(),
		TimerKind:     temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
		CodeVersion:   wf.codeVersion,
		InputDigest:   "ignored",
		Duration:      durationpb.New(time.Minute),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(past),
		CreatedAt:     timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	result, err := runActivity(
		ctx, wf,
		"act:resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		&temporalessv1.RetryPolicy{
			InitialInterval:         durationpb.New(time.Minute),
			BackoffCoefficient:      1.0,
			MaximumAttempts:         3,
			DurableBackoffThreshold: durationpb.New(30 * time.Second),
		},
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
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
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
		policy, wrapperspb.String("x"),
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
		TimerID:    activityRetryTimerID("act:multi"),
	}
	t1, _, err := store.GetTimer(ctx, timerKey)
	if err != nil {
		t.Fatal(err)
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
	record.NextAttemptAt = timestamppb.New(time.Now().UTC().Add(-time.Second))
	if err := store.PutActivity(ctx, record); err != nil {
		t.Fatal(err)
	}

	_, err = runActivity(
		ctx, wf, "act:multi",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy, wrapperspb.String("x"),
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
	if !t2.GetFireAt().AsTime().After(firstFireAt) {
		t.Fatalf("second timer fire_at = %s should be later than first %s", t2.GetFireAt().AsTime(), firstFireAt)
	}
}

func TestDurableRetry_SleepRejectsReservedPrefix(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}
	ctx = context.WithValue(ctx, workflowContextKey{}, wf)

	err := Sleep(ctx, "activity-retry:foo", time.Minute)
	if err == nil {
		t.Fatal("expected error for reserved prefix")
	}
	if !strings.Contains(err.Error(), "activity-retry:") {
		t.Fatalf("unexpected error: %v", err)
	}
}
