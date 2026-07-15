package scanquery_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/scanquery"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListWorkflowsFilters(t *testing.T) {
	ctx := context.Background()
	point, query, _ := newStores(t)
	putWorkflow(t, point, "default", "prices:aapl", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	putWorkflow(t, point, "default", "prices:aapl", "2026-05-05", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)
	putWorkflow(t, point, "default", "prices:msft", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	putWorkflow(t, point, "tenant-b", "prices:aapl", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	tests := []struct {
		name       string
		namespace  string
		workflowID string
		status     temporalessv1.WorkflowStatus
		want       int
	}{
		{name: "all", want: 4},
		{name: "namespace", namespace: "default", want: 3},
		{name: "workflow across namespaces", workflowID: "prices:msft", want: 1},
		{name: "status", status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, want: 3},
		{name: "combined", namespace: "default", workflowID: "prices:aapl", status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, want: 1},
		{name: "missing", namespace: "missing", want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
				Namespace:  test.namespace,
				WorkflowId: test.workflowID,
				Status:     test.status,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(response.GetRecords()); got != test.want {
				t.Fatalf("records = %d, want %d", got, test.want)
			}
		})
	}
}

func TestListActivitiesAcrossAndWithinRuns(t *testing.T) {
	ctx := context.Background()
	point, query, _ := newStores(t)
	for _, test := range []struct {
		key    storage.ActivityKey
		status temporalessv1.ActivityStatus
	}{
		{key: storage.ActivityKey{Namespace: "default", WorkflowID: "wf-a", RunID: "run-1", ActivityID: "a"}, status: temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED},
		{key: storage.ActivityKey{Namespace: "default", WorkflowID: "wf-a", RunID: "run-2", ActivityID: "b"}, status: temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED},
		{key: storage.ActivityKey{Namespace: "tenant-b", WorkflowID: "wf-a", RunID: "run-1", ActivityID: "c"}, status: temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED},
	} {
		if err := point.PutActivity(ctx, &temporalessv1.ActivityRecord{
			SchemaVersion: storage.ActivityRecordSchemaVersion,
			Key:           test.key.Proto(),
			ActivityType:  "activity:test",
			CodeVersion:   "v1",
			Status:        test.status,
		}); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name       string
		namespace  string
		workflowID string
		runID      string
		status     temporalessv1.ActivityStatus
		want       int
	}{
		{name: "all", want: 3},
		{name: "workflow", workflowID: "wf-a", want: 3},
		{name: "namespace workflow", namespace: "default", workflowID: "wf-a", want: 2},
		{name: "single run", namespace: "default", workflowID: "wf-a", runID: "run-1", want: 1},
		{name: "failed", status: temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := query.ListActivitiesQuery(ctx, &temporalessv1.RecordQueryServiceListActivitiesRequest{
				Namespace:  test.namespace,
				WorkflowId: test.workflowID,
				RunId:      test.runID,
				Status:     test.status,
			})
			if err != nil {
				t.Fatal(err)
			}
			if got := len(response.GetRecords()); got != test.want {
				t.Fatalf("records = %d, want %d", got, test.want)
			}
		})
	}
}

func TestScanValidatesPayloadLocation(t *testing.T) {
	ctx := context.Background()
	_, query, operator := newStores(t)
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           storage.NewWorkflowKey("actual", "run").Proto(),
		WorkflowType:  "workflow:test",
		CodeVersion:   "v1",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	misplaced := storage.NewWorkflowKey("misplaced", "run")
	dir, err := misplaced.DirPath()
	if err != nil {
		t.Fatal(err)
	}
	path, err := misplaced.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := operator.CreateDir(dir); err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(path, data); err != nil {
		t.Fatal(err)
	}

	_, err = query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{})
	if err == nil || !strings.Contains(err.Error(), "does not match its storage location") {
		t.Fatalf("error = %v, want payload-location rejection", err)
	}
}

func TestCorePointStoreDoesNotImplementQueryStore(t *testing.T) {
	point, _, _ := newStores(t)
	if _, ok := any(point).(storage.QueryStore); ok {
		t.Fatal("OpenDAL point store must not expose cross-run QueryStore")
	}
}

func TestDueTimersQueryDelegatesToAuthoritativeLedger(t *testing.T) {
	ctx := context.Background()
	point, query, _ := newStores(t)
	now := time.Now().UTC()
	wfKey := storage.NewWorkflowKey("due", "run")
	putWorkflow(t, point, wfKey.Namespace, wfKey.WorkflowID, wfKey.RunID, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS)
	if err := point.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           storage.NewTimerKey(wfKey.WorkflowID, wfKey.RunID, "timer").Proto(),
		TimerKind:     storage.SleepTimerKind,
		CodeVersion:   "v1",
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(now.Add(-time.Minute)),
		CreatedAt:     timestamppb.New(now.Add(-2 * time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}
	response, err := query.DueTimersQuery(ctx, &temporalessv1.RecordQueryServiceDueTimersRequest{
		Now: timestamppb.New(now),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(response.GetDue()); got != 1 {
		t.Fatalf("due timers = %d, want 1", got)
	}
}

func newStores(t *testing.T) (*storage.OpenDALStore, storage.QueryStore, *opendal.Operator) {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	point := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, point, nil)
	if err != nil {
		t.Fatal(err)
	}
	return point, query, operator
}

func putWorkflow(t *testing.T, point storage.Store, namespace, workflowID, runID string, status temporalessv1.WorkflowStatus) {
	t.Helper()
	now := timestamppb.Now()
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key: (&storage.WorkflowKey{
			Namespace:  namespace,
			WorkflowID: workflowID,
			RunID:      runID,
		}).Proto(),
		WorkflowType: "workflow:test",
		CodeVersion:  "v1",
		Status:       status,
		CreatedAt:    now,
	}
	if status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		record.CompletedAt = now
	}
	if err := point.PutWorkflow(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}
