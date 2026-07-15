package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWorkflowExecutionClaimSerializesLiveDuplicates(t *testing.T) {
	tests := []struct {
		name        string
		firstOwner  string
		secondOwner string
	}{
		{
			name:        "distinct owners",
			firstOwner:  "worker-1",
			secondOwner: "worker-2",
		},
		{
			name:        "same owner remains busy",
			firstOwner:  "shared-worker",
			secondOwner: "shared-worker",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claimStore := newTestClaimStore(t)
			firstOptions := &Options{
				WorkflowId:   "single-flight",
				RunId:        "live-duplicate",
				CodeVersion:  "v1",
				ClaimOwnerId: test.firstOwner,
			}
			secondOptions := &Options{
				WorkflowId:   firstOptions.GetWorkflowId(),
				RunId:        firstOptions.GetRunId(),
				CodeVersion:  firstOptions.GetCodeVersion(),
				ClaimOwnerId: test.secondOwner,
			}

			var bodyCalls atomic.Int64
			bodyStarted := make(chan struct{})
			releaseBody := make(chan struct{})
			execute := func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				call := bodyCalls.Add(1)
				if call == 1 {
					close(bodyStarted)
					<-releaseBody
				}
				return wrapperspb.String("completed:" + input.GetValue()), nil
			}

			type runResult struct {
				result *wrapperspb.StringValue
				err    error
			}
			firstDone := make(chan runResult, 1)
			go func() {
				result, err := Run(
					ctx,
					store,
					firstOptions,
					claimStore,
					wrapperspb.String("request"),
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					execute,
				)
				firstDone <- runResult{result: result, err: err}
			}()

			select {
			case <-bodyStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("first workflow body did not start")
			}

			claimKey := workflowExecutionClaimKeyForTest(firstOptions)
			claim, found, err := claimStore.GetClaim(ctx, claimKey)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("workflow execution claim was not stored while the body was running")
			}
			assertWorkflowExecutionClaimForTest(t, claim, claimKey, firstOptions)

			secondResult, secondErr := Run(
				ctx,
				store,
				secondOptions,
				claimStore,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				execute,
			)
			if secondResult != nil {
				t.Fatalf("duplicate result = %v, want nil", secondResult)
			}
			if !errors.Is(secondErr, ErrClaimBusy) {
				t.Fatalf("duplicate error = %v, want ErrClaimBusy", secondErr)
			}
			var busy *ClaimBusyError
			if !errors.As(secondErr, &busy) {
				t.Fatalf("duplicate error type = %T, want *ClaimBusyError", secondErr)
			}
			if busy.ClaimID != WorkflowExecutionClaimID {
				t.Fatalf("busy claim_id = %q, want %q", busy.ClaimID, WorkflowExecutionClaimID)
			}
			if busy.OwnerID != test.firstOwner {
				t.Fatalf("busy owner_id = %q, want %q", busy.OwnerID, test.firstOwner)
			}
			if busy.Capability != storage.CreateOnlyClaims {
				t.Fatalf("busy capability = %v, want %v", busy.Capability, storage.CreateOnlyClaims)
			}
			if got := bodyCalls.Load(); got != 1 {
				t.Fatalf("body calls while duplicate was live = %d, want 1", got)
			}

			// A losing duplicate must not release the winner's claim.
			_, found, err = claimStore.GetClaim(ctx, claimKey)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("losing duplicate removed the live execution claim")
			}

			close(releaseBody)
			var first runResult
			select {
			case first = <-firstDone:
			case <-time.After(5 * time.Second):
				t.Fatal("first workflow did not finish")
			}
			if first.err != nil {
				t.Fatal(first.err)
			}
			if got := first.result.GetValue(); got != "completed:request" {
				t.Fatalf("first result = %q, want %q", got, "completed:request")
			}

			_, found, err = claimStore.GetClaim(ctx, claimKey)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("workflow execution claim remained after completion")
			}
		})
	}
}

func TestWorkflowTerminalReplayIgnoresStaleExecutionClaim(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)
	options := &Options{
		WorkflowId:   "single-flight",
		RunId:        "terminal-replay",
		CodeVersion:  "v1",
		ClaimOwnerId: "first-worker",
	}

	var bodyCalls atomic.Int64
	execute := func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		bodyCalls.Add(1)
		return wrapperspb.String("stored:" + input.GetValue()), nil
	}
	first, err := Run(
		ctx,
		store,
		options,
		claimStore,
		wrapperspb.String("original"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := first.GetValue(); got != "stored:original" {
		t.Fatalf("first result = %q, want %q", got, "stored:original")
	}

	claimKey := workflowExecutionClaimKeyForTest(options)
	staleClaim := workflowExecutionClaimRecordForTest(claimKey, "stale-worker", options)
	created, err := claimStore.TryCreateClaim(ctx, staleClaim)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("failed to create stale workflow execution claim")
	}

	replayOptions := &Options{
		WorkflowId:   options.GetWorkflowId(),
		RunId:        options.GetRunId(),
		CodeVersion:  options.GetCodeVersion(),
		ClaimOwnerId: "replay-worker",
	}
	replayed, err := Run(
		ctx,
		store,
		replayOptions,
		claimStore,
		wrapperspb.String("different-input"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if err != nil {
		t.Fatalf("terminal replay returned stale-claim error: %v", err)
	}
	if got := replayed.GetValue(); got != "stored:original" {
		t.Fatalf("replayed result = %q, want stored terminal result", got)
	}
	if got := bodyCalls.Load(); got != 1 {
		t.Fatalf("body calls = %d, want 1", got)
	}

	// The stale claim remaining proves terminal replay did not need to acquire it.
	existing, found, err := claimStore.GetClaim(ctx, claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found || existing.GetOwnerId() != "stale-worker" {
		t.Fatalf("stale claim after replay = (%v, found=%v), want stale-worker", existing, found)
	}
}

func TestWorkflowExecutionClaimReleasedOnEveryBodyExit(t *testing.T) {
	bodyFailure := errors.New("workflow body failed")
	tests := []struct {
		name       string
		bodyErr    error
		wantErr    error
		wantStatus temporalessv1.WorkflowStatus
	}{
		{
			name:       "timer pending",
			bodyErr:    &TimerPendingError{TimerID: "wait", WakeAt: time.Now().Add(time.Hour)},
			wantErr:    ErrTimerPending,
			wantStatus: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		},
		{
			name:       "event pending",
			bodyErr:    &EventPendingError{EventID: "approval"},
			wantErr:    ErrEventPending,
			wantStatus: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		},
		{
			name:       "activity claim busy",
			bodyErr:    &ClaimBusyError{ClaimID: "activity:send", OwnerID: "activity-worker"},
			wantErr:    ErrClaimBusy,
			wantStatus: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		},
		{
			name: "workflow dependency pending",
			bodyErr: &WorkflowDependencyPendingError{
				WorkflowID: "upstream",
				RunID:      "upstream-run",
			},
			wantErr:    ErrWorkflowDependencyPending,
			wantStatus: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		},
		{
			name:       "terminal failure",
			bodyErr:    bodyFailure,
			wantErr:    bodyFailure,
			wantStatus: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED,
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claimStore := newTestClaimStore(t)
			options := &Options{
				WorkflowId:   "claim-release",
				RunId:        "exit-" + intToASCII(int32(index)),
				CodeVersion:  "v1",
				ClaimOwnerId: "worker-1",
			}

			var bodyCalls atomic.Int64
			result, err := Run(
				ctx,
				store,
				options,
				claimStore,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					bodyCalls.Add(1)
					return nil, test.bodyErr
				},
			)
			if result != nil {
				t.Fatalf("result = %v, want nil", result)
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if got := bodyCalls.Load(); got != 1 {
				t.Fatalf("body calls = %d, want 1", got)
			}

			claimKey := workflowExecutionClaimKeyForTest(options)
			_, found, err := claimStore.GetClaim(ctx, claimKey)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("workflow execution claim remained after body returned")
			}

			workflowRecord, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
				Namespace:  storage.DefaultNamespace,
				WorkflowID: options.GetWorkflowId(),
				RunID:      options.GetRunId(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("workflow record was not stored")
			}
			if got := workflowRecord.GetStatus(); got != test.wantStatus {
				t.Fatalf("workflow status = %v, want %v", got, test.wantStatus)
			}

			// A direct create succeeding is an independent proof that the prior
			// invocation released the exact deterministic claim key.
			created, err := claimStore.TryCreateClaim(
				ctx,
				workflowExecutionClaimRecordForTest(claimKey, "next-worker", options),
			)
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Fatal("next worker could not acquire the released workflow execution claim")
			}
		})
	}
}

func TestWorkflowExecutionClaimReleasedAfterContextCancellation(t *testing.T) {
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)
	options := &Options{
		WorkflowId:   "claim-release",
		RunId:        "context-canceled",
		CodeVersion:  "v1",
		ClaimOwnerId: "cancelled-worker",
	}
	ctx, cancel := context.WithCancel(context.Background())
	bodyStarted := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		_, err := Run(
			ctx,
			store,
			options,
			claimStore,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				close(bodyStarted)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		)
		done <- err
	}()

	select {
	case <-bodyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow body did not start")
	}
	claimKey := workflowExecutionClaimKeyForTest(options)
	_, found, err := claimStore.GetClaim(context.Background(), claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("workflow execution claim was not held before cancellation")
	}

	cancel()
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not return after context cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	_, found, err = claimStore.GetClaim(context.Background(), claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("workflow execution claim leaked after parent context cancellation")
	}
}

func TestWorkflowExecutionClaimReleaseFailureIsSurfacedWithContextValues(t *testing.T) {
	store := newTestStore(t)
	releaseErr := errors.New("claim backend unavailable")
	claimStore := &failingDeleteClaimStore{
		ClaimStore: newTestClaimStore(t),
		deleteErr:  releaseErr,
		wantValue:  "tenant-auth",
	}
	options := &Options{
		WorkflowId:   "claim-release",
		RunId:        "release-error",
		CodeVersion:  "v1",
		ClaimOwnerId: "worker",
	}
	valueCtx := context.WithValue(context.Background(), releaseContextKey{}, "tenant-auth")
	ctx, cancel := context.WithCancel(valueCtx)

	_, err := Run(
		ctx,
		store,
		options,
		claimStore,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			cancel()
			return nil, &TimerPendingError{TimerID: "wait", WakeAt: time.Now().Add(time.Hour)}
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("error = %v, want joined ErrTimerPending", err)
	}
	if !errors.Is(err, releaseErr) {
		t.Fatalf("error = %v, want joined release error", err)
	}
	if !errors.Is(err, ErrClaimRelease) {
		t.Fatalf("error = %v, want ErrClaimRelease", err)
	}
	if !claimStore.sawValue {
		t.Fatal("claim release lost request-scoped context values")
	}

	_, found, getErr := claimStore.GetClaim(context.Background(), workflowExecutionClaimKeyForTest(options))
	if getErr != nil {
		t.Fatal(getErr)
	}
	if !found {
		t.Fatal("failing delete unexpectedly removed the workflow execution claim")
	}
}

func TestWorkflowExecutionClaimRefreshesTerminalStateAroundAcquisition(t *testing.T) {
	tests := []struct {
		name            string
		forceCreateLoss bool
	}{
		{
			name:            "terminal appears after failed create",
			forceCreateLoss: true,
		},
		{
			name:            "terminal appears immediately before successful create",
			forceCreateLoss: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			options := &Options{
				WorkflowId:   "claim-race",
				RunId:        "terminal-refresh",
				CodeVersion:  "v1",
				ClaimOwnerId: "racing-worker",
			}
			claimStore := &terminalRaceClaimStore{
				ClaimStore:      newTestClaimStore(t),
				forceCreateLoss: test.forceCreateLoss,
				beforeCreate: func() error {
					return putCompletedWorkflowForClaimRace(ctx, store, options, "stored:race")
				},
			}

			var bodyCalls atomic.Int64
			result, err := Run(
				ctx,
				store,
				options,
				claimStore,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
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

			_, found, err := claimStore.GetClaim(ctx, workflowExecutionClaimKeyForTest(options))
			if err != nil {
				t.Fatal(err)
			}
			if found {
				t.Fatal("workflow execution claim remained after terminal replay race")
			}
		})
	}
}

func TestExpiredWorkflowExecutionClaimRemainsBusy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)
	options := &Options{
		WorkflowId:   "claim-expiry",
		RunId:        "create-only",
		CodeVersion:  "v1",
		ClaimOwnerId: "new-worker",
	}
	claimKey := workflowExecutionClaimKeyForTest(options)
	expired := workflowExecutionClaimRecordForTest(claimKey, "stale-worker", options)
	expired.LeaseExpiresAt = timestamppb.New(time.Now().Add(-time.Minute))
	created, err := claimStore.TryCreateClaim(ctx, expired)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("failed to seed expired create-only claim")
	}

	var bodyCalls atomic.Int64
	_, err = Run(
		ctx,
		store,
		options,
		claimStore,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			bodyCalls.Add(1)
			return wrapperspb.String("should-not-run"), nil
		},
	)
	var busy *ClaimBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("error = %T (%v), want *ClaimBusyError", err, err)
	}
	if busy.OwnerID != "stale-worker" || !busy.LeaseExpiresAt.Before(time.Now()) {
		t.Fatalf("busy claim = %#v, want expired stale-worker claim", busy)
	}
	if got := bodyCalls.Load(); got != 0 {
		t.Fatalf("body calls = %d, want 0", got)
	}
	_, found, err := claimStore.GetClaim(ctx, claimKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expired create-only claim was taken over or deleted")
	}
}

func TestClaimStoreWithoutOwnerKeepsAtLeastOnceExecution(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)
	options := &Options{
		WorkflowId:  "claim-opt-in",
		RunId:       "no-owner",
		CodeVersion: "v1",
	}
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var bodyCalls atomic.Int64
	execute := func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		call := bodyCalls.Add(1)
		if call == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return wrapperspb.String("call:" + intToASCII(int32(call))), nil
	}

	type runResult struct {
		result *wrapperspb.StringValue
		err    error
	}
	firstDone := make(chan runResult, 1)
	go func() {
		result, err := Run(
			ctx, store, options, claimStore,
			wrapperspb.String("request"),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			execute,
		)
		firstDone <- runResult{result: result, err: err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first workflow body did not start")
	}

	second, err := Run(
		ctx, store, options, claimStore,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		execute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := second.GetValue(); got != "call:2" {
		t.Fatalf("second result = %q, want call:2", got)
	}
	close(releaseFirst)
	first := <-firstDone
	if first.err != nil {
		t.Fatal(first.err)
	}
	if got := bodyCalls.Load(); got != 2 {
		t.Fatalf("body calls = %d, want 2 without claim_owner_id", got)
	}

	_, found, err := claimStore.GetClaim(ctx, workflowExecutionClaimKeyForTest(options))
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("claim store alone unexpectedly enabled workflow execution claims")
	}
}

type terminalRaceClaimStore struct {
	storage.ClaimStore
	once            sync.Once
	beforeCreate    func() error
	beforeCreateErr error
	forceCreateLoss bool
}

type releaseContextKey struct{}

type failingDeleteClaimStore struct {
	storage.ClaimStore
	deleteErr error
	wantValue any
	sawValue  bool
}

func (store *failingDeleteClaimStore) DeleteClaim(
	ctx context.Context,
	_ storage.ClaimKey,
) (bool, error) {
	store.sawValue = ctx.Value(releaseContextKey{}) == store.wantValue && ctx.Err() == nil
	return false, store.deleteErr
}

func (store *terminalRaceClaimStore) TryCreateClaim(
	ctx context.Context,
	record *temporalessv1.ClaimRecord,
) (bool, error) {
	if record.GetResourceType() != temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW {
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

func putCompletedWorkflowForClaimRace(
	ctx context.Context,
	store storage.Store,
	options *Options,
	value string,
) error {
	input := wrapperspb.String("request")
	result := wrapperspb.String(value)
	inputAny, err := anypb.New(input)
	if err != nil {
		return err
	}
	resultAny, err := anypb.New(result)
	if err != nil {
		return err
	}
	now := timestamppb.Now()
	return store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key: (&storage.WorkflowKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: options.GetWorkflowId(),
			RunID:      options.GetRunId(),
		}).Proto(),
		WorkflowType: messagePairType("workflow", input, &wrapperspb.StringValue{}),
		CodeVersion:  options.GetCodeVersion(),
		Input:        inputAny,
		Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		Result:       resultAny,
		CreatedAt:    now,
		CompletedAt:  now,
	})
}

func workflowExecutionClaimKeyForTest(options *Options) storage.ClaimKey {
	return storage.ClaimKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: options.GetWorkflowId(),
		RunID:      options.GetRunId(),
		ClaimID:    WorkflowExecutionClaimID,
	}
}

func workflowExecutionClaimRecordForTest(
	key storage.ClaimKey,
	ownerID string,
	options *Options,
) *temporalessv1.ClaimRecord {
	now := time.Now().UTC()
	return &temporalessv1.ClaimRecord{
		SchemaVersion:  storage.ClaimRecordSchemaVersion,
		Key:            key.Proto(),
		OwnerId:        ownerID,
		ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:     options.GetWorkflowId(),
		CodeVersion:    options.GetCodeVersion(),
		LeaseExpiresAt: timestamppb.New(now.Add(DefaultClaimLeaseDuration)),
		CreatedAt:      timestamppb.New(now),
		HeartbeatAt:    timestamppb.New(now),
	}
}

func assertWorkflowExecutionClaimForTest(
	t *testing.T,
	claim *temporalessv1.ClaimRecord,
	wantKey storage.ClaimKey,
	options *Options,
) {
	t.Helper()
	if got := claim.GetSchemaVersion(); got != storage.ClaimRecordSchemaVersion {
		t.Fatalf("claim schema_version = %v, want %v", got, storage.ClaimRecordSchemaVersion)
	}
	key := storage.ClaimKeyFromProto(claim.GetKey())
	if key != wantKey {
		t.Fatalf("claim key = %#v, want %#v", key, wantKey)
	}
	if got := claim.GetOwnerId(); got != options.GetClaimOwnerId() {
		t.Fatalf("claim owner_id = %q, want %q", got, options.GetClaimOwnerId())
	}
	if got := claim.GetResourceType(); got != temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW {
		t.Fatalf("claim resource_type = %v, want WORKFLOW", got)
	}
	if got := claim.GetResourceId(); got != options.GetWorkflowId() {
		t.Fatalf("claim resource_id = %q, want %q", got, options.GetWorkflowId())
	}
	if got := claim.GetCodeVersion(); got != options.GetCodeVersion() {
		t.Fatalf("claim code_version = %q, want %q", got, options.GetCodeVersion())
	}
	if claim.GetCreatedAt() == nil || claim.GetHeartbeatAt() == nil || claim.GetLeaseExpiresAt() == nil {
		t.Fatal("claim timestamps must all be populated")
	}
	createdAt := claim.GetCreatedAt().AsTime()
	if heartbeatAt := claim.GetHeartbeatAt().AsTime(); !heartbeatAt.Equal(createdAt) {
		t.Fatalf("claim heartbeat_at = %s, want created_at %s", heartbeatAt, createdAt)
	}
	if got := claim.GetLeaseExpiresAt().AsTime().Sub(createdAt); got != DefaultClaimLeaseDuration {
		t.Fatalf("claim lease duration = %s, want %s", got, DefaultClaimLeaseDuration)
	}
}
