package janitor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	"github.com/jim-technologies/temporaless/adapters/go/scanquery"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestSweepDeletesOldCompletedRuns(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Run 1: completed yesterday — should be swept.
	runWorkflow(t, ctx, store, "prices:old", "2026-05-03")
	backdate(t, ctx, store, "prices:old", "2026-05-03", time.Now().Add(-48*time.Hour))

	// Run 2: completed just now — should be kept.
	runWorkflow(t, ctx, store, "prices:fresh", "2026-05-04")

	// Run 3: still in progress — should be kept.
	leaveInProgress(t, ctx, store, "prices:waiting", "2026-05-04")

	deleted, err := janitor.Sweep(ctx, query, store, nil, sweepRequest(time.Now(), 24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:old",
		RunID:      "2026-05-03",
	}); found {
		t.Fatal("expected old run to be deleted")
	}
	if _, found, _ := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:old",
		RunID:      "2026-05-03",
		ActivityID: "fetch:AAPL",
	}); found {
		t.Fatal("expected old activity to be deleted")
	}
	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:fresh",
		RunID:      "2026-05-04",
	}); !found {
		t.Fatal("expected fresh run to be kept")
	}
	if _, found, _ := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:waiting",
		RunID:      "2026-05-04",
	}); !found {
		t.Fatal("expected in-progress run to be kept")
	}
}

func TestSweepRejectsBadInput(t *testing.T) {
	root := t.TempDir()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, store, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := janitor.Sweep(context.Background(), query, store, nil, sweepRequest(time.Now(), 0)); err == nil {
		t.Fatal("expected error for non-positive maxAge")
	}
}

type pointOnlyClaimStore struct {
	storage.ClaimStore
}

type noClaimsPointStore struct {
	storage.ClaimStore
}

func (store *noClaimsPointStore) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return storage.NoClaims, nil
}

type casClaimsPointStore struct {
	storage.ClaimStore
}

func (store *casClaimsPointStore) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return storage.CASClaims, nil
}

type corruptClaimListingStore struct {
	storage.ClaimRunStore
	corruptFor  storage.WorkflowKey
	corrupt     *temporalessv1.ClaimRecord
	deleteCalls int
}

type combinedRecordClaimStore struct {
	storage.Store
	storage.ClaimRunStore
}

type staticWorkflowQuery struct {
	records []*temporalessv1.WorkflowRecord
}

func (query staticWorkflowQuery) ListWorkflows(
	context.Context,
	*temporalessv1.ListWorkflowsRequest,
) (*temporalessv1.ListWorkflowsResponse, error) {
	return &temporalessv1.ListWorkflowsResponse{Records: query.records}, nil
}

func TestSweepDoesNotTrustStaleCompletedQueryRecord(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreAndQuery(t)
	key := storage.NewWorkflowKey("prices:reset", "run:old")
	runWorkflow(t, ctx, store, key.WorkflowID, key.RunID)
	backdate(t, ctx, store, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))

	staleCompleted, found, err := store.GetWorkflow(ctx, key)
	if err != nil || !found {
		t.Fatalf("get completed workflow: found=%v err=%v", found, err)
	}
	// Simulate a reset that the derived query adapter has not indexed yet.
	authoritative := &temporalessv1.WorkflowRecord{
		SchemaVersion: staleCompleted.GetSchemaVersion(),
		Key:           staleCompleted.GetKey(),
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		WorkflowType:  staleCompleted.GetWorkflowType(),
		Input:         staleCompleted.GetInput(),
		CreatedAt:     staleCompleted.GetCreatedAt(),
	}
	if err := store.PutWorkflow(ctx, authoritative); err != nil {
		t.Fatal(err)
	}

	deleted, err := janitor.Sweep(
		ctx,
		staticWorkflowQuery{records: []*temporalessv1.WorkflowRecord{staleCompleted}},
		store,
		nil,
		sweepRequest(time.Now(), 24*time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
	if record, found, err := store.GetWorkflow(ctx, key); err != nil || !found || record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("authoritative workflow after sweep: record=%v found=%v err=%v", record, found, err)
	}
	activityKey := storage.NewActivityKey(key.WorkflowID, key.RunID, "fetch:AAPL")
	if _, found, err := store.GetActivity(ctx, activityKey); err != nil || !found {
		t.Fatalf("activity after sweep: found=%v err=%v", found, err)
	}
}

func (store *corruptClaimListingStore) ListClaims(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error) {
	records, err := store.ClaimRunStore.ListClaims(ctx, key)
	if err != nil {
		return nil, err
	}
	if key.Namespace == store.corruptFor.Namespace && key.WorkflowID == store.corruptFor.WorkflowID && key.RunID == store.corruptFor.RunID {
		records = append(records, store.corrupt)
	}
	return records, nil
}

func (store *corruptClaimListingStore) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	store.deleteCalls++
	return store.ClaimRunStore.DeleteClaim(ctx, key)
}

func TestSweepDeletesClaims(t *testing.T) {
	tests := []struct {
		name       string
		autodetect bool
	}{
		{name: "explicit separate claim store"},
		{name: "auto-detected claim store", autodetect: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			recordStore, query := newTestStoreAndQuery(t)
			claimStore := newClaimStore(t)
			key := storage.NewWorkflowKey("prices:claimed", "run:old")
			runWorkflow(t, ctx, recordStore, key.WorkflowID, key.RunID)
			backdate(t, ctx, recordStore, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))
			claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "stale")
			createClaim(t, ctx, claimStore, claimKey)

			var store storage.Store = recordStore
			var explicitClaims storage.ClaimStore = claimStore
			if test.autodetect {
				store = &combinedRecordClaimStore{Store: recordStore, ClaimRunStore: claimStore}
				explicitClaims = nil
			}
			deleted, err := janitor.Sweep(ctx, query, store, explicitClaims, sweepRequest(time.Now(), 24*time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if deleted != 1 {
				t.Fatalf("deleted = %d, want 1", deleted)
			}
			if _, found, err := recordStore.GetWorkflow(ctx, key); err != nil || found {
				t.Fatalf("workflow after sweep: found=%v err=%v", found, err)
			}
			if _, found, err := claimStore.GetClaim(ctx, claimKey); err != nil || found {
				t.Fatalf("claim after sweep: found=%v err=%v", found, err)
			}
		})
	}
}

func TestSweepRejectsPointOnlyClaimStoreBeforeMutation(t *testing.T) {
	ctx := context.Background()
	recordStore, query := newTestStoreAndQuery(t)
	baseClaims := newClaimStore(t)
	claims := &pointOnlyClaimStore{ClaimStore: baseClaims}
	key := storage.NewWorkflowKey("prices:point-only", "run:old")
	runWorkflow(t, ctx, recordStore, key.WorkflowID, key.RunID)
	backdate(t, ctx, recordStore, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain")
	createClaim(t, ctx, baseClaims, claimKey)

	deleted, err := janitor.Sweep(ctx, query, recordStore, claims, sweepRequest(time.Now(), 24*time.Hour))
	if !errors.Is(err, janitor.ErrClaimRunListingUnsupported) {
		t.Fatalf("error = %v, want ErrClaimRunListingUnsupported", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
	assertRunAndClaimPresent(t, ctx, recordStore, baseClaims, key, claimKey)
}

func TestSweepTreatsNoClaimsCapabilityAsRecordOnly(t *testing.T) {
	ctx := context.Background()
	recordStore, query := newTestStoreAndQuery(t)
	baseClaims := newClaimStore(t)
	claims := &noClaimsPointStore{ClaimStore: baseClaims}
	key := storage.NewWorkflowKey("prices:no-claims", "run:old")
	runWorkflow(t, ctx, recordStore, key.WorkflowID, key.RunID)
	backdate(t, ctx, recordStore, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "out-of-contract")
	createClaim(t, ctx, baseClaims, claimKey)

	deleted, err := janitor.Sweep(ctx, query, recordStore, claims, sweepRequest(time.Now(), 24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if _, found, err := recordStore.GetWorkflow(ctx, key); err != nil || found {
		t.Fatalf("workflow after sweep: found=%v err=%v", found, err)
	}
	if _, found, err := baseClaims.GetClaim(ctx, claimKey); err != nil || !found {
		t.Fatalf("NO_CLAIMS backend record should be ignored: found=%v err=%v", found, err)
	}
}

func TestSweepRejectsReservedCASCapabilityBeforeMutation(t *testing.T) {
	ctx := context.Background()
	recordStore, query := newTestStoreAndQuery(t)
	baseClaims := newClaimStore(t)
	claims := &casClaimsPointStore{ClaimStore: baseClaims}
	key := storage.NewWorkflowKey("prices:cas", "run:old")
	runWorkflow(t, ctx, recordStore, key.WorkflowID, key.RunID)
	backdate(t, ctx, recordStore, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain")
	createClaim(t, ctx, baseClaims, claimKey)

	deleted, err := janitor.Sweep(
		ctx,
		query,
		recordStore,
		claims,
		sweepRequest(time.Now(), 24*time.Hour),
	)
	if !errors.Is(err, janitor.ErrClaimCapabilityUnsupported) {
		t.Fatalf("error = %v, want ErrClaimCapabilityUnsupported", err)
	}
	if deleted != 0 {
		t.Fatalf("deleted = %d, want 0", deleted)
	}
	assertRunAndClaimPresent(t, ctx, recordStore, baseClaims, key, claimKey)
}

func TestSweepRejectsCorruptClaimSnapshotsBeforeAnyRunMutation(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(storage.WorkflowKey) *temporalessv1.ClaimRecord
	}{
		{
			name: "invalid key",
			corrupt: func(key storage.WorkflowKey) *temporalessv1.ClaimRecord {
				return &temporalessv1.ClaimRecord{Key: &temporalessv1.ClaimKey{
					Namespace:  key.Namespace,
					WorkflowId: key.WorkflowID,
					RunId:      key.RunID,
					ClaimId:    "invalid/id",
				}}
			},
		},
		{
			name: "misplaced namespace",
			corrupt: func(key storage.WorkflowKey) *temporalessv1.ClaimRecord {
				claimKey := storage.ClaimKey{Namespace: "other", WorkflowID: key.WorkflowID, RunID: key.RunID, ClaimID: "misplaced"}
				return &temporalessv1.ClaimRecord{Key: claimKey.Proto()}
			},
		},
		{
			name: "misplaced workflow",
			corrupt: func(key storage.WorkflowKey) *temporalessv1.ClaimRecord {
				return &temporalessv1.ClaimRecord{Key: storage.NewClaimKey("prices:other", key.RunID, "misplaced").Proto()}
			},
		},
		{
			name: "misplaced run",
			corrupt: func(key storage.WorkflowKey) *temporalessv1.ClaimRecord {
				return &temporalessv1.ClaimRecord{Key: storage.NewClaimKey(key.WorkflowID, "run:other", "misplaced").Proto()}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			recordStore, query := newTestStoreAndQuery(t)
			baseClaims := newClaimStore(t)
			first := storage.NewWorkflowKey("prices:first", "run:old")
			corruptKey := storage.NewWorkflowKey("prices:corrupt", "run:old")
			for _, key := range []storage.WorkflowKey{first, corruptKey} {
				runWorkflow(t, ctx, recordStore, key.WorkflowID, key.RunID)
				backdate(t, ctx, recordStore, key.WorkflowID, key.RunID, time.Now().Add(-48*time.Hour))
				createClaim(t, ctx, baseClaims, storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain"))
			}
			claims := &corruptClaimListingStore{
				ClaimRunStore: baseClaims,
				corruptFor:    corruptKey,
				corrupt:       test.corrupt(corruptKey),
			}

			deleted, err := janitor.Sweep(ctx, query, recordStore, claims, sweepRequest(time.Now(), 24*time.Hour))
			if !errors.Is(err, janitor.ErrRunListingDataLoss) {
				t.Fatalf("error = %v, want ErrRunListingDataLoss", err)
			}
			if deleted != 0 {
				t.Fatalf("deleted = %d, want 0", deleted)
			}
			if claims.deleteCalls != 0 {
				t.Fatalf("DeleteClaim calls = %d, want 0", claims.deleteCalls)
			}
			for _, key := range []storage.WorkflowKey{first, corruptKey} {
				claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain")
				assertRunAndClaimPresent(t, ctx, recordStore, baseClaims, key, claimKey)
			}
		})
	}
}

func newTestStoreAndQuery(t *testing.T) (*storage.OpenDALStore, storage.WorkflowQueryStore) {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store, query
}

func newClaimStore(t *testing.T) storage.ClaimRunStore {
	t.Helper()
	bucket, err := fileblob.OpenBucket(t.TempDir(), &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return gocdkclaims.NewStore(bucket)
}

func createClaim(t *testing.T, ctx context.Context, store storage.ClaimStore, key storage.ClaimKey) {
	t.Helper()
	created, err := store.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           key.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	})
	if err != nil || !created {
		t.Fatalf("create claim: created=%v err=%v", created, err)
	}
}

func assertRunAndClaimPresent(
	t *testing.T,
	ctx context.Context,
	recordStore storage.Store,
	claimStore storage.ClaimStore,
	key storage.WorkflowKey,
	claimKey storage.ClaimKey,
) {
	t.Helper()
	if _, found, err := recordStore.GetWorkflow(ctx, key); err != nil || !found {
		t.Fatalf("workflow mutated before rejection: found=%v err=%v", found, err)
	}
	activityKey := storage.NewActivityKey(key.WorkflowID, key.RunID, "fetch:AAPL")
	if _, found, err := recordStore.GetActivity(ctx, activityKey); err != nil || !found {
		t.Fatalf("activity mutated before rejection: found=%v err=%v", found, err)
	}
	if _, found, err := claimStore.GetClaim(ctx, claimKey); err != nil || !found {
		t.Fatalf("claim mutated before rejection: found=%v err=%v", found, err)
	}
}

func sweepRequest(now time.Time, maxAge time.Duration) *temporalessv1.SweepRequest {
	return &temporalessv1.SweepRequest{
		Now:    timestamppb.New(now),
		MaxAge: durationpb.New(maxAge),
	}
}

func runWorkflow(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string) {
	t.Helper()
	_, err := workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: workflowID, RunId: runID},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return workflow.ExecuteActivity(
				ctx,
				&workflow.ActivityOptions{ActivityId: "fetch:" + input.GetValue()},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("ok:" + request.GetValue()), nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func leaveInProgress(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string) {
	t.Helper()
	_, _ = workflow.Run(
		ctx,
		store,
		&workflow.Options{WorkflowId: workflowID, RunId: runID},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := workflow.Sleep(ctx, "wait", time.Hour); err != nil {
				return nil, err
			}
			return wrapperspb.String("done"), nil
		},
	)
}

// backdate sets completed_at on a stored workflow record so the test can pretend
// the record is older than it really is. Real callers never need this — only
// tests that exercise retention thresholds.
func backdate(t *testing.T, ctx context.Context, store storage.Store, workflowID, runID string, completedAt time.Time) {
	t.Helper()
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
	record, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("expected stored record for %s/%s", workflowID, runID)
	}
	record.CompletedAt = timestamppb.New(completedAt)
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", record.GetStatus())
	}
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
}
