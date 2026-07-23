package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLatestWorkflowRunPointerUsesCallerRunOrderForBackfills(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	workflowID := "prices:aapl"
	newerOrder := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	olderOrder := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	putLatestPointerWorkflowWithOrder(t, store, workflowID, "opaque:newer", time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC), newerOrder)
	putLatestPointerWorkflowWithOrder(t, store, workflowID, "opaque:backfill", time.Date(2026, 7, 3, 1, 0, 0, 0, time.UTC), olderOrder)

	pointer, found, err := store.GetLatestWorkflowRun(ctx, "", workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("latest-run pointer not found")
	}
	if got := pointer.GetKey().GetRunId(); got != "opaque:newer" {
		t.Fatalf("latest run_id = %q, want %q", got, "opaque:newer")
	}
	if pointer.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("status = %s", pointer.GetStatus())
	}
	wantRecordTime := time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC)
	if got := pointer.GetRecordTime().AsTime(); !got.Equal(wantRecordTime) {
		t.Fatalf("record_time = %s, want %s", got, wantRecordTime)
	}
	if got := pointer.GetRunOrderTime().AsTime(); !got.Equal(newerOrder) {
		t.Fatalf("run_order_time = %s, want %s", got, newerOrder)
	}
	path, err := latestWorkflowRunPointerPath("", workflowID)
	if err != nil {
		t.Fatal(err)
	}
	if path != "temporaless/v2/default/_latest/prices:aapl.binpb" {
		t.Fatalf("pointer path = %q", path)
	}
	exists, err := operator.IsExist(path)
	if err != nil || !exists {
		t.Fatalf("pointer object: exists=%v err=%v", exists, err)
	}
}

func TestLatestWorkflowRunPointerFallsBackToRecordTime(t *testing.T) {
	ctx := context.Background()
	store, _ := newLatestPointerTestStore(t)
	putLatestPointerWorkflow(t, store, "pipeline", "batch:two", time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC))
	putLatestPointerWorkflow(t, store, "pipeline", "batch:one", time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))

	pointer, found, err := store.GetLatestWorkflowRun(ctx, "default", "pipeline")
	if err != nil || !found {
		t.Fatalf("GetLatestWorkflowRun: found=%v err=%v", found, err)
	}
	if got := pointer.GetKey().GetRunId(); got != "batch:one" {
		t.Fatalf("latest run_id = %q", got)
	}
}

func TestLatestWorkflowRunPointerTieUsesRecordTimeWithoutParsingRunID(t *testing.T) {
	ctx := context.Background()
	store, _ := newLatestPointerTestStore(t)
	runOrder := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	putLatestPointerWorkflowWithOrder(t, store, "pipeline", "9999-date-looking", time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC), runOrder)
	putLatestPointerWorkflowWithOrder(t, store, "pipeline", "0000-opaque-later-write", time.Date(2026, 7, 4, 0, 0, 0, 0, time.UTC), runOrder)

	pointer, found, err := store.GetLatestWorkflowRun(ctx, "default", "pipeline")
	if err != nil || !found {
		t.Fatalf("GetLatestWorkflowRun: found=%v err=%v", found, err)
	}
	if got := pointer.GetKey().GetRunId(); got != "0000-opaque-later-write" {
		t.Fatalf("latest run_id = %q", got)
	}
}

func TestDeleteWorkflowRetainsDerivedLatestPointer(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	putLatestPointerWorkflow(t, store, "prices:aapl", "2026-07-01", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	putLatestPointerWorkflow(t, store, "prices:aapl", "2026-07-02", time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC))

	deleted, err := store.DeleteWorkflow(ctx, WorkflowKey{WorkflowID: "prices:aapl", RunID: "2026-07-01"})
	if err != nil || !deleted {
		t.Fatalf("delete older run: deleted=%v err=%v", deleted, err)
	}
	pointer, found, err := store.GetLatestWorkflowRun(ctx, "", "prices:aapl")
	if err != nil || !found || pointer.GetKey().GetRunId() != "2026-07-02" {
		t.Fatalf("pointer after older delete: pointer=%v found=%v err=%v", pointer, found, err)
	}

	deleted, err = store.DeleteWorkflow(ctx, WorkflowKey{WorkflowID: "prices:aapl", RunID: "2026-07-02"})
	if err != nil || !deleted {
		t.Fatalf("delete latest run: deleted=%v err=%v", deleted, err)
	}
	_, found, err = store.GetLatestWorkflowRun(ctx, "", "prices:aapl")
	if err != nil || found {
		t.Fatalf("pointer after latest delete: found=%v err=%v", found, err)
	}
	pointerPath, err := latestWorkflowRunPointerPath("", "prices:aapl")
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := operator.IsExist(pointerPath); err != nil || !exists {
		t.Fatalf("derived pointer should be retained: exists=%v err=%v", exists, err)
	}
}

func TestDeleteWorkflowLeavesCorruptPointerInPlace(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	target := WorkflowKey{WorkflowID: "prices:aapl", RunID: "2026-07-01"}
	other := WorkflowKey{WorkflowID: "prices:msft", RunID: "2026-07-02"}
	putLatestPointerWorkflow(t, store, target.WorkflowID, target.RunID, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	putLatestPointerWorkflow(t, store, other.WorkflowID, other.RunID, time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC))

	targetPointerPath, err := latestWorkflowRunPointerPath(target.Namespace, target.WorkflowID)
	if err != nil {
		t.Fatal(err)
	}
	misplaced := &temporalessv1.LatestWorkflowRunPointer{
		Key:        other.Proto(),
		Status:     temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		RecordTime: timestamppb.New(time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)),
		UpdatedAt:  timestamppb.Now(),
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(misplaced)
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(targetPointerPath, data); err != nil {
		t.Fatal(err)
	}

	deleted, err := store.DeleteWorkflow(ctx, target)
	if !deleted || err != nil {
		t.Fatalf("delete with corrupt pointer: deleted=%v err=%v", deleted, err)
	}
	exists, existsErr := operator.IsExist(targetPointerPath)
	if existsErr != nil || !exists {
		t.Fatalf("corrupt pointer should remain: exists=%v err=%v", exists, existsErr)
	}
	if _, found, getErr := store.GetWorkflow(ctx, other); getErr != nil || !found {
		t.Fatalf("other workflow was redirected/deleted: found=%v err=%v", found, getErr)
	}
}

func TestLatestWorkflowRunPointerSkipsUnspecifiedStatus(t *testing.T) {
	ctx := context.Background()
	store, _ := newLatestPointerTestStore(t)
	key := NewWorkflowKey("pipeline", "run")
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
	}); err != nil {
		t.Fatal(err)
	}
	_, found, err := store.GetLatestWorkflowRun(ctx, "", key.WorkflowID)
	if err != nil || found {
		t.Fatalf("unspecified status pointer: found=%v err=%v", found, err)
	}
}

func TestLatestWorkflowRunPointerPublicReadHidesStaleMetadata(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name   string
		mutate func(*temporalessv1.LatestWorkflowRunPointer)
	}{
		{
			name: "status mismatch",
			mutate: func(pointer *temporalessv1.LatestWorkflowRunPointer) {
				pointer.Status = temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS
			},
		},
		{
			name: "record time mismatch",
			mutate: func(pointer *temporalessv1.LatestWorkflowRunPointer) {
				pointer.RecordTime = timestamppb.New(pointer.GetRecordTime().AsTime().Add(time.Second))
			},
		},
		{
			name: "run order time mismatch",
			mutate: func(pointer *temporalessv1.LatestWorkflowRunPointer) {
				pointer.RunOrderTime = timestamppb.New(pointer.GetRunOrderTime().AsTime().Add(time.Second))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, operator := newLatestPointerTestStore(t)
			putLatestPointerWorkflow(t, store, "pipeline", "run", time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
			path, err := latestWorkflowRunPointerPath("", "pipeline")
			if err != nil {
				t.Fatal(err)
			}
			data, err := operator.Read(path)
			if err != nil {
				t.Fatal(err)
			}
			pointer := &temporalessv1.LatestWorkflowRunPointer{}
			if err := proto.Unmarshal(data, pointer); err != nil {
				t.Fatal(err)
			}
			test.mutate(pointer)
			data, err = proto.MarshalOptions{Deterministic: true}.Marshal(pointer)
			if err != nil {
				t.Fatal(err)
			}
			if err := operator.Write(path, data); err != nil {
				t.Fatal(err)
			}

			_, found, err := store.GetLatestWorkflowRun(ctx, "", "pipeline")
			if found || err != nil {
				t.Fatalf("found=%v err=%v, want stale pointer hidden as not-found", found, err)
			}
		})
	}
}

func TestLatestWorkflowRunPointerTransitionWindowIsNotFound(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	key := NewWorkflowKey("pipeline", "run")
	createdAt := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
	runOrder := timestamppb.New(createdAt)
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     timestamppb.New(createdAt),
		RunOrderTime:  runOrder,
	}
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}

	record.Status = temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	record.CompletedAt = timestamppb.New(createdAt.Add(time.Minute))
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	path, err := key.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(path, data); err != nil {
		t.Fatal(err)
	}

	_, found, err := store.GetLatestWorkflowRun(ctx, "", key.WorkflowID)
	if err != nil || found {
		t.Fatalf("transition window: found=%v err=%v, want not-found", found, err)
	}
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	pointer, found, err := store.GetLatestWorkflowRun(ctx, "", key.WorkflowID)
	if err != nil || !found || pointer.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("updated pointer=%v found=%v err=%v", pointer, found, err)
	}
}

func TestLatestWorkflowRunPointerMalformedShapeIsCorrupt(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	putLatestPointerWorkflow(t, store, "pipeline", "run", time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	path, err := latestWorkflowRunPointerPath("", "pipeline")
	if err != nil {
		t.Fatal(err)
	}
	data, err := operator.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	pointer := &temporalessv1.LatestWorkflowRunPointer{}
	if err := proto.Unmarshal(data, pointer); err != nil {
		t.Fatal(err)
	}
	pointer.RunOrderTime = nil
	data, err = proto.MarshalOptions{Deterministic: true}.Marshal(pointer)
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(path, data); err != nil {
		t.Fatal(err)
	}

	_, found, err := store.GetLatestWorkflowRun(ctx, "", "pipeline")
	if found || !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("found=%v err=%v, want false/ErrCorruptRecord", found, err)
	}
}

func TestLatestWorkflowRunPointerMissingReferenceIsNotFound(t *testing.T) {
	ctx := context.Background()
	store, operator := newLatestPointerTestStore(t)
	putLatestPointerWorkflow(t, store, "pipeline", "run", time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC))
	path, err := latestWorkflowRunPointerPath("", "pipeline")
	if err != nil {
		t.Fatal(err)
	}
	data, err := operator.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	pointer := &temporalessv1.LatestWorkflowRunPointer{}
	if err := proto.Unmarshal(data, pointer); err != nil {
		t.Fatal(err)
	}
	pointer.Key.RunId = "missing-run"
	data, err = proto.MarshalOptions{Deterministic: true}.Marshal(pointer)
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(path, data); err != nil {
		t.Fatal(err)
	}

	_, found, err := store.GetLatestWorkflowRun(ctx, "", "pipeline")
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want not found", found, err)
	}
}

func TestLatestWorkflowRunPointerWriterAdvancesAfterStatusTransition(t *testing.T) {
	ctx := context.Background()
	store, _ := newLatestPointerTestStore(t)
	key := NewWorkflowKey("pipeline", "run")
	createdAt := time.Date(2026, 7, 3, 0, 0, 0, 0, time.UTC)
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     timestamppb.New(createdAt),
		RunOrderTime:  timestamppb.New(createdAt.Add(-time.Hour)),
	}
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	record.Status = temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	record.CompletedAt = timestamppb.New(createdAt.Add(time.Minute))
	if err := store.PutWorkflow(ctx, record); err != nil {
		t.Fatal(err)
	}
	pointer, found, err := store.GetLatestWorkflowRun(ctx, "", key.WorkflowID)
	if err != nil || !found || pointer.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("pointer=%v found=%v err=%v", pointer, found, err)
	}
	if !pointer.GetRunOrderTime().AsTime().Equal(record.GetRunOrderTime().AsTime()) {
		t.Fatalf("run_order_time=%v, want %v", pointer.GetRunOrderTime(), record.GetRunOrderTime())
	}
}

func newLatestPointerTestStore(t *testing.T) (*OpenDALStore, *opendal.Operator) {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return NewOpenDALStore(operator), operator
}

func putLatestPointerWorkflow(
	t *testing.T,
	store *OpenDALStore,
	workflowID string,
	runID string,
	completedAt time.Time,
) {
	putLatestPointerWorkflowWithOrder(t, store, workflowID, runID, completedAt, time.Time{})
}

func putLatestPointerWorkflowWithOrder(
	t *testing.T,
	store *OpenDALStore,
	workflowID string,
	runID string,
	completedAt time.Time,
	runOrderTime time.Time,
) {
	t.Helper()
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: WorkflowRecordSchemaVersion,
		Key:           NewWorkflowKey(workflowID, runID).Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     timestamppb.New(completedAt.Add(-time.Minute)),
		CompletedAt:   timestamppb.New(completedAt),
	}
	if !runOrderTime.IsZero() {
		record.RunOrderTime = timestamppb.New(runOrderTime)
	}
	if err := store.PutWorkflow(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}
