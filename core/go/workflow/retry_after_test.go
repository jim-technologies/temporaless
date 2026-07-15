package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// ActivityError.RetryAfter is the activity body's way to surface a
// vendor-supplied minimum wait (HTTP `Retry-After`, OpenAI rate-limit reset,
// etc.). The retry planner uses max(computed_interval, RetryAfter) so vendor
// pacing wins over the configured exponential schedule. When combined with
// RetryPolicy.DurableBackoffThreshold, a long Retry-After value automatically
// becomes a durable timer rather than burning serverless compute.

func TestRetryAfter_LongerThanComputedIntervalWins(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}
	// Tiny interval but Retry-After is 30s — durable threshold is 5s, so
	// this should become a durable wait at ~30s.
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(100 * time.Millisecond),
		BackoffCoefficient:      1.0,
		MaximumInterval:         durationpb.New(100 * time.Millisecond),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(5 * time.Second),
	}
	attemptCount := 0
	start := time.Now().UTC()
	_, err := runActivity(
		ctx, wf, "act:retry-after",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:retry-after"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			attemptCount++
			return nil, NewRetryableActivityError("rate_limited", "vendor 429", 30*time.Second, nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("err = %v, want pending (durable retry)", err)
	}
	if attemptCount != 1 {
		t.Fatalf("attempts = %d, want 1", attemptCount)
	}
	var tp *TimerPendingError
	if !errors.As(err, &tp) {
		t.Fatalf("err type = %T", err)
	}
	// Wake-at should be ~30s out, not the 100ms computed interval.
	delta := tp.WakeAt.Sub(start)
	if delta < 29*time.Second {
		t.Fatalf("wake_at - start = %s, want ~30s", delta)
	}

	// Persisted ActivityAttempt.failure must carry the retry_after duration.
	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "act:retry-after",
	}
	record, _, err := store.GetActivity(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.GetAttempts()) != 1 {
		t.Fatalf("attempts = %d", len(record.GetAttempts()))
	}
	gotRA := record.GetAttempts()[0].GetFailure().GetRetryAfter().AsDuration()
	if gotRA != 30*time.Second {
		t.Fatalf("persisted retry_after = %s, want 30s", gotRA)
	}
}

func TestRetryAfter_ShorterThanComputedIntervalIgnored(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}
	// 10ms in-process retry policy. Retry-After is 1ms — must not shorten
	// the wait below the configured floor.
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:    durationpb.New(10 * time.Millisecond),
		BackoffCoefficient: 1.0,
		MaximumInterval:    durationpb.New(10 * time.Millisecond),
		MaximumAttempts:    3,
	}
	attemptCount := 0
	start := time.Now()
	_, err := runActivity(
		ctx, wf, "act:short-ra",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:short-ra"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			attemptCount++
			if attemptCount < 3 {
				return nil, NewRetryableActivityError("flaky", "", 1*time.Millisecond, nil)
			}
			return wrapperspb.String("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if attemptCount != 3 {
		t.Fatalf("attempts = %d", attemptCount)
	}
	// Elapsed must reflect the floor (3 attempts × 10ms = 20ms between retries).
	if elapsed := time.Since(start); elapsed < 18*time.Millisecond {
		t.Fatalf("elapsed = %s — Retry-After undercut the configured floor", elapsed)
	}
}

func TestRetryAfter_TurnsShortPolicyIntoDurableWait(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "wf",
		runID:       "r",
		codeVersion: "test",
	}
	// Policy says "retry in 1 second", durable threshold is 30s. Without
	// Retry-After this would be in-process. With Retry-After: 600s the
	// effective interval crosses the threshold → durable.
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(1 * time.Second),
		BackoffCoefficient:      1.0,
		MaximumInterval:         durationpb.New(1 * time.Second),
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(30 * time.Second),
	}
	_, err := runActivity(
		ctx, wf, "act:promote",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("act:promote"),
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			return nil, NewRetryableActivityError("rate_limited", "x", 10*time.Minute, nil)
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("expected pending (durable retry from Retry-After), got %v", err)
	}
	timerKey := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		TimerID:    testRetryTimerID("act:promote"),
	}
	timer, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found {
		t.Fatalf("expected durable retry timer: err=%v found=%v", err, found)
	}
	delay := time.Until(timer.GetFireAt().AsTime())
	if delay < 9*time.Minute {
		t.Fatalf("timer fire_at delay = %s, want ~10min", delay)
	}
}
