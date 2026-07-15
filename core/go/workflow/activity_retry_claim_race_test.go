package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// preparedTimerClaimRaceStore deterministically models the timer-first claim
// handoff: the first contender loses to a holder that publishes a retry timer
// and releases without an ActivityRecord, then the contender acquires on its
// second create attempt.
type preparedTimerClaimRaceStore struct {
	storage.ClaimStore
	prepare func(context.Context) error
	calls   atomic.Int32
}

func (store *preparedTimerClaimRaceStore) TryCreateClaim(
	ctx context.Context,
	record *temporalessv1.ClaimRecord,
) (bool, error) {
	if record.GetResourceType() != temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY ||
		store.calls.Add(1) != 1 {
		return store.ClaimStore.TryCreateClaim(ctx, record)
	}

	winner := proto.Clone(record).(*temporalessv1.ClaimRecord)
	winner.OwnerId = "winner"
	created, err := store.ClaimStore.TryCreateClaim(ctx, winner)
	if err != nil {
		return false, err
	}
	if !created {
		return false, fmt.Errorf("failed to create simulated winning claim")
	}
	if err := store.prepare(ctx); err != nil {
		return false, err
	}
	if _, err := store.DeleteClaim(ctx, storage.ClaimKeyFromProto(record.GetKey())); err != nil {
		return false, err
	}
	return false, nil
}

func TestActivityClaimRechecksPreparedRetryTimerAfterAcquisition(t *testing.T) {
	ctx := context.Background()
	recordStore := newTestStore(t)
	claimStore := &preparedTimerClaimRaceStore{ClaimStore: newTestClaimStore(t)}
	workflow := &Workflow{
		store:           recordStore,
		claimStore:      claimStore,
		claimCapability: storage.CreateOnlyClaims,
		workflowID:      "activity-claim",
		runID:           "prepared-timer-handoff",
		codeVersion:     "v1",
		claimOwner:      "contender",
	}
	activityID := "send"
	retryTimerID := "retry:send"
	wakeAt := time.Now().UTC().Add(time.Hour)
	claimStore.prepare = func(ctx context.Context) error {
		return recordStore.PutTimer(ctx, &temporalessv1.TimerRecord{
			SchemaVersion:   storage.TimerRecordSchemaVersion,
			Key:             storage.NewTimerKey(workflow.workflowID, workflow.runID, retryTimerID).Proto(),
			TimerKind:       temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
			CodeVersion:     workflow.codeVersion,
			Duration:        durationpb.New(time.Hour),
			Status:          temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			FireAt:          timestamppb.New(wakeAt),
			CreatedAt:       timestamppb.Now(),
			RetryActivityId: activityID,
		})
	}

	var executions atomic.Int32
	_, err := runActivity(
		ctx,
		workflow,
		activityID,
		activityClaimTestType,
		&temporalessv1.RetryPolicy{
			InitialInterval:         durationpb.New(time.Hour),
			BackoffCoefficient:      1,
			MaximumAttempts:         3,
			DurableBackoffThreshold: durationpb.New(time.Second),
		},
		retryTimerID,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions.Add(1)
			return wrapperspb.String("duplicate"), nil
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("error = %v, want timer pending", err)
	}
	if got := executions.Load(); got != 0 {
		t.Fatalf("activity executions = %d, want 0", got)
	}
	if claimStore.calls.Load() != 2 {
		t.Fatalf("claim create calls = %d, want 2", claimStore.calls.Load())
	}
	if _, found, getErr := recordStore.GetActivity(
		ctx,
		storage.NewActivityKey(workflow.workflowID, workflow.runID, activityID),
	); getErr != nil || found {
		t.Fatalf("activity after prepared handoff: found=%v err=%v, want absent", found, getErr)
	}
	if _, found, getErr := claimStore.GetClaim(ctx, activityClaimKeyForTest(workflow, activityID)); getErr != nil || found {
		t.Fatalf("claim after prepared handoff: found=%v err=%v, want released", found, getErr)
	}
}

func TestActivityClaimRechecksNewerPreparedTimerAgainstLaggingRetryingRecord(t *testing.T) {
	ctx := context.Background()
	recordStore := newTestStore(t)
	claimStore := &preparedTimerClaimRaceStore{ClaimStore: newTestClaimStore(t)}
	workflow := &Workflow{
		store:           recordStore,
		claimStore:      claimStore,
		claimCapability: storage.CreateOnlyClaims,
		workflowID:      "activity-claim",
		runID:           "lagging-retrying-handoff",
		codeVersion:     "v1",
		claimOwner:      "contender",
	}
	activityID := "send"
	retryTimerID := "retry:send"
	policy := &temporalessv1.RetryPolicy{
		InitialInterval:         durationpb.New(time.Second),
		BackoffCoefficient:      120,
		MaximumAttempts:         3,
		DurableBackoffThreshold: durationpb.New(time.Minute),
	}
	plan, err := planRetries(policy)
	if err != nil {
		t.Fatal(err)
	}
	input, err := anypb.New(wrapperspb.String("request"))
	if err != nil {
		t.Fatal(err)
	}
	now := timestamppb.Now()
	failure := &temporalessv1.ActivityFailure{Code: "transient", Message: "try again"}
	activityKey := storage.NewActivityKey(workflow.workflowID, workflow.runID, activityID)
	if err := recordStore.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           activityKey.Proto(),
		ActivityType:  activityClaimTestType,
		CodeVersion:   workflow.codeVersion,
		Input:         input,
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		Failure:       proto.Clone(failure).(*temporalessv1.ActivityFailure),
		CreatedAt:     now,
		Attempts: []*temporalessv1.ActivityAttempt{
			{
				Attempt:     1,
				StartedAt:   now,
				CompletedAt: now,
				Failure:     failure,
			},
		},
		RetryPolicy:  normalizeRetryPolicy(policy, plan),
		RetryTimerId: retryTimerID,
	}); err != nil {
		t.Fatal(err)
	}

	wakeAt := time.Now().UTC().Add(2 * time.Minute)
	claimStore.prepare = func(ctx context.Context) error {
		return recordStore.PutTimer(ctx, &temporalessv1.TimerRecord{
			SchemaVersion:   storage.TimerRecordSchemaVersion,
			Key:             storage.NewTimerKey(workflow.workflowID, workflow.runID, retryTimerID).Proto(),
			TimerKind:       temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
			CodeVersion:     workflow.codeVersion,
			Duration:        durationpb.New(2 * time.Minute),
			Status:          temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			FireAt:          timestamppb.New(wakeAt),
			CreatedAt:       timestamppb.Now(),
			RetryActivityId: activityID,
		})
	}

	var executions atomic.Int32
	_, err = runActivity(
		ctx,
		workflow,
		activityID,
		activityClaimTestType,
		policy,
		retryTimerID,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions.Add(1)
			return wrapperspb.String("duplicate"), nil
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("error = %v, want timer pending", err)
	}
	if got := executions.Load(); got != 0 {
		t.Fatalf("activity executions = %d, want 0", got)
	}
	stored, found, getErr := recordStore.GetActivity(ctx, activityKey)
	if getErr != nil || !found {
		t.Fatalf("lagging activity: found=%v err=%v", found, getErr)
	}
	if stored.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING ||
		len(stored.GetAttempts()) != 1 || stored.GetNextAttemptAt() != nil {
		t.Fatalf("lagging activity was overwritten: %v", stored)
	}
	if _, found, getErr := claimStore.GetClaim(ctx, activityClaimKeyForTest(workflow, activityID)); getErr != nil || found {
		t.Fatalf("claim after lagging handoff: found=%v err=%v, want released", found, getErr)
	}
}
