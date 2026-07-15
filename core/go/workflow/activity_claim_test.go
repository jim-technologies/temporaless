package workflow

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const activityClaimTestType = "activity:google.protobuf.StringValue->google.protobuf.StringValue"

func TestActivityClaimSerializesLiveDuplicatesIncludingSameOwner(t *testing.T) {
	tests := []struct {
		name        string
		firstOwner  string
		secondOwner string
	}{
		{name: "distinct owners", firstOwner: "worker-1", secondOwner: "worker-2"},
		{name: "same owner", firstOwner: "shared-worker", secondOwner: "shared-worker"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rawStore := newTestStore(t)
			scope := storage.NewWorkflowKey("activity-claim", "live-duplicate")
			cache := newRunScopedCache(rawStore, scope)
			claimStore := newTestClaimStore(t)
			firstWorkflow := &Workflow{
				store:       cache,
				claimStore:  claimStore,
				workflowID:  scope.WorkflowID,
				runID:       scope.RunID,
				codeVersion: "v1",
				claimOwner:  test.firstOwner,
			}
			secondWorkflow := &Workflow{
				store:       cache,
				claimStore:  claimStore,
				workflowID:  scope.WorkflowID,
				runID:       scope.RunID,
				codeVersion: "v1",
				claimOwner:  test.secondOwner,
			}

			var bodyCalls atomic.Int64
			bodyStarted := make(chan struct{})
			releaseBody := make(chan struct{})
			execute := func(_ context.Context) (*wrapperspb.StringValue, error) {
				call := bodyCalls.Add(1)
				if call == 1 {
					close(bodyStarted)
					<-releaseBody
				}
				return wrapperspb.String("done"), nil
			}

			type activityResult struct {
				result *wrapperspb.StringValue
				err    error
			}
			firstDone := make(chan activityResult, 1)
			go func() {
				result, err := runActivity(
					ctx, firstWorkflow, "send", activityClaimTestType, nil,
					"",
					wrapperspb.String("request"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					execute,
				)
				firstDone <- activityResult{result: result, err: err}
			}()

			select {
			case <-bodyStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("first activity body did not start")
			}

			second, err := runActivity(
				ctx, secondWorkflow, "send", activityClaimTestType, nil,
				"",
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				execute,
			)
			if second != nil {
				t.Fatalf("duplicate result = %v, want nil", second)
			}
			var busy *ClaimBusyError
			if !errors.As(err, &busy) {
				t.Fatalf("duplicate error = %T (%v), want *ClaimBusyError", err, err)
			}
			if busy.OwnerID != test.firstOwner {
				t.Fatalf("busy owner_id = %q, want %q", busy.OwnerID, test.firstOwner)
			}
			if got := bodyCalls.Load(); got != 1 {
				t.Fatalf("body calls while first is live = %d, want 1", got)
			}

			close(releaseBody)
			select {
			case first := <-firstDone:
				if first.err != nil {
					t.Fatal(first.err)
				}
				if got := first.result.GetValue(); got != "done" {
					t.Fatalf("first result = %q, want done", got)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("first activity did not finish")
			}

			_, found, getErr := claimStore.GetClaim(ctx, activityClaimKeyForTest(firstWorkflow, "send"))
			if getErr != nil {
				t.Fatal(getErr)
			}
			if found {
				t.Fatal("terminal activity claim was not released")
			}
		})
	}
}

func TestActivityClaimReleasedAtDurableBoundaries(t *testing.T) {
	bodyFailure := errors.New("activity failed")
	tests := []struct {
		name       string
		policy     *RetryPolicy
		execute    func(context.Context) (*wrapperspb.StringValue, error)
		wantErr    error
		wantStatus temporalessv1.ActivityStatus
	}{
		{
			name: "completed",
			execute: func(context.Context) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("ok"), nil
			},
			wantStatus: temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		},
		{
			name: "terminal failure",
			execute: func(context.Context) (*wrapperspb.StringValue, error) {
				return nil, bodyFailure
			},
			wantErr:    bodyFailure,
			wantStatus: temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED,
		},
		{
			name: "durable retry pending",
			policy: &RetryPolicy{
				MaximumAttempts:         2,
				InitialInterval:         durationpb.New(time.Hour),
				DurableBackoffThreshold: durationpb.New(time.Second),
			},
			execute: func(context.Context) (*wrapperspb.StringValue, error) {
				return nil, bodyFailure
			},
			wantErr:    ErrTimerPending,
			wantStatus: temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claimStore := newTestClaimStore(t)
			workflow := &Workflow{
				store:       store,
				claimStore:  claimStore,
				workflowID:  "activity-boundary",
				runID:       "run-" + intToASCII(int32(index)),
				codeVersion: "v1",
				claimOwner:  "worker",
			}

			result, err := runActivity(
				ctx, workflow, "call", activityClaimTestType, test.policy,
				testRetryTimerID("call"),
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				test.execute,
			)
			if test.wantErr == nil {
				if err != nil {
					t.Fatal(err)
				}
				if got := result.GetValue(); got != "ok" {
					t.Fatalf("result = %q, want ok", got)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}

			claimKey := activityClaimKeyForTest(workflow, "call")
			_, found, getErr := claimStore.GetClaim(ctx, claimKey)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if found {
				t.Fatal("activity claim remained after durable boundary")
			}

			record, found, getErr := store.GetActivity(ctx, storage.NewActivityKey(
				workflow.workflowID, workflow.runID, "call",
			))
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !found {
				t.Fatal("durable activity record was not stored")
			}
			if got := record.GetStatus(); got != test.wantStatus {
				t.Fatalf("activity status = %v, want %v", got, test.wantStatus)
			}
		})
	}
}

func TestActivityClaimRetainedWhenOutcomeIsAmbiguous(t *testing.T) {
	writeErr := errors.New("activity record write failed")
	tests := []struct {
		name string
		run  func(*testing.T, *Workflow) error
	}{
		{
			name: "terminal storage failure",
			run: func(t *testing.T, workflow *Workflow) error {
				t.Helper()
				_, err := runActivity(
					context.Background(), workflow, "call", activityClaimTestType, nil,
					"",
					wrapperspb.String("request"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context) (*wrapperspb.StringValue, error) {
						return wrapperspb.String("side-effect-completed"), nil
					},
				)
				return err
			},
		},
		{
			name: "execution cancellation before record persistence",
			run: func(t *testing.T, workflow *Workflow) error {
				t.Helper()
				ctx, cancel := context.WithCancel(context.Background())
				_, err := runActivity(
					ctx, workflow, "call", activityClaimTestType, nil,
					"",
					wrapperspb.String("request"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context) (*wrapperspb.StringValue, error) {
						cancel()
						return nil, context.Canceled
					},
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rawStore := newTestStore(t)
			var store storage.Store = rawStore
			if test.name == "terminal storage failure" {
				store = &failingActivityPutStore{Store: rawStore, err: writeErr}
			}
			claimStore := newTestClaimStore(t)
			workflow := &Workflow{
				store:       store,
				claimStore:  claimStore,
				workflowID:  "activity-ambiguous",
				runID:       strings.ReplaceAll(test.name, " ", "-"),
				codeVersion: "v1",
				claimOwner:  "worker",
			}

			err := test.run(t, workflow)
			if err == nil {
				t.Fatal("expected ambiguous execution/storage error")
			}
			claim, found, getErr := claimStore.GetClaim(
				context.Background(), activityClaimKeyForTest(workflow, "call"),
			)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if !found || claim.GetOwnerId() != "worker" {
				t.Fatalf("retained claim = (%v, found=%v), want worker claim", claim, found)
			}
		})
	}
}

func TestActivityClaimReleaseFailureLeavesWorkflowInProgress(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	releaseErr := errors.New("activity claim delete failed")
	claimStore := &failingActivityDeleteClaimStore{
		ClaimStore: newTestClaimStore(t),
		err:        releaseErr,
	}
	options := &Options{
		WorkflowId:   "activity-release",
		RunId:        "release-failed",
		CodeVersion:  "v1",
		ClaimOwnerId: "worker",
	}

	_, err := Run(
		ctx, store, options, claimStore,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return ExecuteActivity(
				ctx, &ActivityOptions{ActivityId: "call"}, input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("stored"), nil
				},
			)
		},
	)
	if !errors.Is(err, ErrClaimRelease) || !errors.Is(err, releaseErr) {
		t.Fatalf("error = %v, want joined ErrClaimRelease and backend error", err)
	}

	wfRecord, found, getErr := store.GetWorkflow(ctx, storage.NewWorkflowKey(
		options.GetWorkflowId(), options.GetRunId(),
	))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !found || wfRecord.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("workflow record = (%v, found=%v), want IN_PROGRESS", wfRecord, found)
	}
	activityRecord, found, getErr := store.GetActivity(ctx, storage.NewActivityKey(
		options.GetWorkflowId(), options.GetRunId(), "call",
	))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !found || activityRecord.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("activity record = (%v, found=%v), want COMPLETED", activityRecord, found)
	}
	_, found, getErr = claimStore.GetClaim(ctx, storage.NewClaimKey(
		options.GetWorkflowId(), options.GetRunId(), ActivityClaimIDPrefix+"call",
	))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !found {
		t.Fatal("failed activity claim release unexpectedly removed the claim")
	}
	_, found, getErr = claimStore.GetClaim(ctx, storage.NewClaimKey(
		options.GetWorkflowId(), options.GetRunId(), WorkflowExecutionClaimID,
	))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if found {
		t.Fatal("workflow execution claim was not released")
	}
}

func TestActivityClaimRefreshBypassesCachedMiss(t *testing.T) {
	tests := []struct {
		name            string
		forceCreateLoss bool
	}{
		{name: "terminal appears before failed create", forceCreateLoss: true},
		{name: "terminal appears before successful create", forceCreateLoss: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rawStore := newTestStore(t)
			scope := storage.NewWorkflowKey("activity-refresh", strings.ReplaceAll(test.name, " ", "-"))
			cache := newRunScopedCache(rawStore, scope)
			workflow := &Workflow{
				store:       cache,
				workflowID:  scope.WorkflowID,
				runID:       scope.RunID,
				codeVersion: "v1",
				claimOwner:  "worker",
			}
			claimStore := &activityTerminalRaceClaimStore{
				ClaimStore:      newTestClaimStore(t),
				forceCreateLoss: test.forceCreateLoss,
				beforeCreate: func() error {
					return putCompletedActivityForClaimRace(
						ctx, rawStore, workflow, "call", "stored:race",
					)
				},
			}
			workflow.claimStore = claimStore

			var bodyCalls atomic.Int64
			result, err := runActivity(
				ctx, workflow, "call", activityClaimTestType, nil,
				"",
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context) (*wrapperspb.StringValue, error) {
					bodyCalls.Add(1)
					return wrapperspb.String("should-not-run"), nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if got := result.GetValue(); got != "stored:race" {
				t.Fatalf("result = %q, want stored:race", got)
			}
			if got := bodyCalls.Load(); got != 0 {
				t.Fatalf("body calls = %d, want 0", got)
			}
			_, found, getErr := claimStore.GetClaim(ctx, activityClaimKeyForTest(workflow, "call"))
			if getErr != nil {
				t.Fatal(getErr)
			}
			if found {
				t.Fatal("activity claim remained after terminal refresh")
			}
		})
	}
}

type failingActivityPutStore struct {
	storage.Store
	err error
}

func (store *failingActivityPutStore) PutActivity(
	context.Context,
	*temporalessv1.ActivityRecord,
) error {
	return store.err
}

type failingActivityDeleteClaimStore struct {
	storage.ClaimStore
	err error
}

func (store *failingActivityDeleteClaimStore) DeleteClaim(
	ctx context.Context,
	key storage.ClaimKey,
) (bool, error) {
	if strings.HasPrefix(key.ClaimID, ActivityClaimIDPrefix) {
		return false, store.err
	}
	return store.ClaimStore.DeleteClaim(ctx, key)
}

type activityTerminalRaceClaimStore struct {
	storage.ClaimStore
	once            sync.Once
	beforeCreate    func() error
	beforeCreateErr error
	forceCreateLoss bool
}

func (store *activityTerminalRaceClaimStore) TryCreateClaim(
	ctx context.Context,
	record *temporalessv1.ClaimRecord,
) (bool, error) {
	if record.GetResourceType() != temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY {
		return store.ClaimStore.TryCreateClaim(ctx, record)
	}
	store.once.Do(func() {
		store.beforeCreateErr = store.beforeCreate()
	})
	if store.beforeCreateErr != nil {
		return false, store.beforeCreateErr
	}
	if store.forceCreateLoss {
		return false, nil
	}
	return store.ClaimStore.TryCreateClaim(ctx, record)
}

func putCompletedActivityForClaimRace(
	ctx context.Context,
	store storage.Store,
	workflow *Workflow,
	activityID string,
	value string,
) error {
	inputAny, err := anypb.New(wrapperspb.String("request"))
	if err != nil {
		return err
	}
	resultAny, err := anypb.New(wrapperspb.String(value))
	if err != nil {
		return err
	}
	now := timestamppb.Now()
	return store.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.NewActivityKey(
			workflow.workflowID, workflow.runID, activityID,
		).Proto(),
		ActivityType: activityClaimTestType,
		CodeVersion:  workflow.codeVersion,
		Input:        inputAny,
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		Result:       resultAny,
		CreatedAt:    now,
		CompletedAt:  now,
	})
}

func activityClaimKeyForTest(workflow *Workflow, activityID string) storage.ClaimKey {
	return storage.NewClaimKey(
		workflow.workflowID,
		workflow.runID,
		ActivityClaimIDPrefix+activityID,
	)
}
