package storage_test

import (
	"context"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func newStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}

func putWorkflow(t *testing.T, store *storage.OpenDALStore, namespace, workflowID, runID string, status temporalessv1.WorkflowStatus) {
	t.Helper()
	ctx := context.Background()
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key: storage.WorkflowKey{
			Namespace:  namespace,
			WorkflowID: workflowID,
			RunID:      runID,
		}.Proto(),
		WorkflowType: "test:type",
		CodeVersion:  "v1",
		InputDigest:  "digest",
		Status:       status,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestListWorkflowsFilters(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	putWorkflow(t, store, "default", "prices:aapl", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	putWorkflow(t, store, "default", "prices:aapl", "2026-05-05", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED)
	putWorkflow(t, store, "default", "prices:msft", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	putWorkflow(t, store, "tenant-b", "prices:aapl", "2026-05-04", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	tests := []struct {
		name       string
		namespace  string
		workflowID string
		status     temporalessv1.WorkflowStatus
		wantCount  int
	}{
		{"no filters", "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 4},
		{"namespace only", "default", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 3},
		{"namespace + workflow id", "default", "prices:aapl", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 2},
		{"status only", "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, 3},
		{"namespace + status", "default", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, 1},
		{"namespace + workflow id + status", "default", "prices:aapl", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, 1},
		{"empty namespace + workflow id falls back to client filter", "", "prices:msft", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 1},
		{"unknown namespace returns empty", "missing", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 0},
		{"unknown workflow id returns empty", "default", "prices:tsla", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records, err := store.ListWorkflows(ctx, test.namespace, test.workflowID, test.status)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(records); got != test.wantCount {
				t.Fatalf("count = %d, want %d", got, test.wantCount)
			}
		})
	}
}

func TestDeleteWorkflowIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:delete",
		RunID:      "2026-05-04",
	}

	putWorkflow(t, store, key.Namespace, key.WorkflowID, key.RunID, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	deleted, err := store.DeleteWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("first delete should report deleted=true")
	}

	deleted, err = store.DeleteWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if deleted {
		t.Fatal("second delete should report deleted=false (idempotent)")
	}

	_, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("workflow record should be gone after delete")
	}
}

func TestListActivitiesScopedToWorkflowRun(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	wfA := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf-a", RunID: "2026-05-04"}
	wfB := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf-b", RunID: "2026-05-04"}

	putActivity(t, store, wfA, "fetch:1")
	putActivity(t, store, wfA, "fetch:2")
	putActivity(t, store, wfB, "fetch:1")

	a, err := store.ListActivities(ctx, wfA)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 2 {
		t.Fatalf("wf-a activities = %d, want 2", len(a))
	}

	b, err := store.ListActivities(ctx, wfB)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != 1 {
		t.Fatalf("wf-b activities = %d, want 1", len(b))
	}

	empty, err := store.ListActivities(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf-missing",
		RunID:      "2026-05-04",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("missing workflow activities = %d, want 0", len(empty))
	}
}

func putActivity(t *testing.T, store *storage.OpenDALStore, wf storage.WorkflowKey, activityID string) {
	t.Helper()
	resultAny, err := anypb.New(wrapperspb.String("ok"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PutActivity(context.Background(), &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.ActivityKey{
			Namespace:  wf.Namespace,
			WorkflowID: wf.WorkflowID,
			RunID:      wf.RunID,
			ActivityID: activityID,
		}.Proto(),
		ActivityType: "test:activity",
		CodeVersion:  "v1",
		InputDigest:  "digest",
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		Result:       resultAny,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestListTimersFiltersByStatus(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	key := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf", RunID: "run"}

	putTimer(t, store, key, "scheduled-1", temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	putTimer(t, store, key, "scheduled-2", temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	putTimer(t, store, key, "fired-1", temporalessv1.TimerStatus_TIMER_STATUS_FIRED)

	all, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("all timers = %d, want 3", len(all))
	}

	scheduled, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	if err != nil {
		t.Fatal(err)
	}
	if len(scheduled) != 2 {
		t.Fatalf("scheduled timers = %d, want 2", len(scheduled))
	}
}

func putTimer(t *testing.T, store *storage.OpenDALStore, wf storage.WorkflowKey, timerID string, status temporalessv1.TimerStatus) {
	t.Helper()
	if err := store.PutTimer(context.Background(), &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key: storage.TimerKey{
			Namespace:  wf.Namespace,
			WorkflowID: wf.WorkflowID,
			RunID:      wf.RunID,
			TimerID:    timerID,
		}.Proto(),
		TimerKind:   storage.SleepTimerKind,
		CodeVersion: "v1",
		InputDigest: "digest",
		Status:      status,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteRecordsAreIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	wf := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf", RunID: "run"}

	putActivity(t, store, wf, "activity-1")
	putTimer(t, store, wf, "timer-1", temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)

	// Activity
	deleted, err := store.DeleteActivity(ctx, storage.ActivityKey{
		Namespace:  wf.Namespace,
		WorkflowID: wf.WorkflowID,
		RunID:      wf.RunID,
		ActivityID: "activity-1",
	})
	if err != nil || !deleted {
		t.Fatalf("first activity delete: deleted=%v err=%v", deleted, err)
	}
	deleted, err = store.DeleteActivity(ctx, storage.ActivityKey{
		Namespace:  wf.Namespace,
		WorkflowID: wf.WorkflowID,
		RunID:      wf.RunID,
		ActivityID: "activity-1",
	})
	if err != nil || deleted {
		t.Fatalf("repeat activity delete: deleted=%v err=%v (want false/nil)", deleted, err)
	}

	// Timer
	deleted, err = store.DeleteTimer(ctx, storage.TimerKey{
		Namespace:  wf.Namespace,
		WorkflowID: wf.WorkflowID,
		RunID:      wf.RunID,
		TimerID:    "timer-1",
	})
	if err != nil || !deleted {
		t.Fatalf("first timer delete: deleted=%v err=%v", deleted, err)
	}
	deleted, err = store.DeleteTimer(ctx, storage.TimerKey{
		Namespace:  wf.Namespace,
		WorkflowID: wf.WorkflowID,
		RunID:      wf.RunID,
		TimerID:    "timer-1",
	})
	if err != nil || deleted {
		t.Fatalf("repeat timer delete: deleted=%v err=%v (want false/nil)", deleted, err)
	}
}

func TestListEventsScopedToWorkflowRun(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)
	wf := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf", RunID: "run"}

	for _, eventID := range []string{"approval", "rejection", "timeout"} {
		if err := storage.SendEvent(ctx, store, storage.EventKey{
			Namespace:  wf.Namespace,
			WorkflowID: wf.WorkflowID,
			RunID:      wf.RunID,
			EventID:    eventID,
		}, wrapperspb.String("payload-"+eventID)); err != nil {
			t.Fatal(err)
		}
	}

	events, err := store.ListEvents(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}

	deleted, err := store.DeleteEvent(ctx, storage.EventKey{
		Namespace:  wf.Namespace,
		WorkflowID: wf.WorkflowID,
		RunID:      wf.RunID,
		EventID:    "approval",
	})
	if err != nil || !deleted {
		t.Fatalf("delete event: deleted=%v err=%v", deleted, err)
	}

	events, err = store.ListEvents(ctx, wf)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("events after delete = %d, want 2", len(events))
	}
}

func TestStoreOperationsRespectCancelledContext(t *testing.T) {
	store := newStore(t)
	wf := storage.WorkflowKey{Namespace: storage.DefaultNamespace, WorkflowID: "wf", RunID: "run"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := store.GetWorkflow(ctx, wf); err == nil {
		t.Fatal("expected ctx error from GetWorkflow")
	}
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{Key: wf.Proto()}); err == nil {
		t.Fatal("expected ctx error from PutWorkflow")
	}
	if _, err := store.ListWorkflows(ctx, "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED); err == nil {
		t.Fatal("expected ctx error from ListWorkflows")
	}
}

func TestSendEventValidatesArguments(t *testing.T) {
	store := newStore(t)
	ctx := context.Background()

	if err := storage.SendEvent(ctx, store, storage.EventKey{
		WorkflowID: "wf",
		RunID:      "run",
		EventID:    ".",
	}, wrapperspb.String("p")); err == nil {
		t.Fatal("expected error from invalid key")
	}
}
