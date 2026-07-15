package storage

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOpenDALStorePointReadsRejectMisplacedAndWrongSchemaPayloads(t *testing.T) {
	workflowKey := NewWorkflowKey("workflow", "run")
	otherWorkflowKey := NewWorkflowKey("other-workflow", "other-run")
	activityKey := NewActivityKey("workflow", "run", "activity")
	otherActivityKey := NewActivityKey("other-workflow", "other-run", "other-activity")
	timerKey := NewTimerKey("workflow", "run", "timer")
	otherTimerKey := NewTimerKey("other-workflow", "other-run", "other-timer")
	eventKey := NewEventKey("workflow", "run", "event")
	otherEventKey := NewEventKey("other-workflow", "other-run", "other-event")

	tests := []struct {
		name      string
		path      func() (string, error)
		misplaced proto.Message
		wrong     proto.Message
		get       func(context.Context, *OpenDALStore) (bool, error)
	}{
		{
			name: "workflow",
			path: workflowKey.Path,
			misplaced: &temporalessv1.WorkflowRecord{
				SchemaVersion: WorkflowRecordSchemaVersion,
				Key:           otherWorkflowKey.Proto(),
			},
			wrong: &temporalessv1.WorkflowRecord{
				SchemaVersion: ActivityRecordSchemaVersion,
				Key:           workflowKey.Proto(),
			},
			get: func(ctx context.Context, store *OpenDALStore) (bool, error) {
				_, found, err := store.GetWorkflow(ctx, workflowKey)
				return found, err
			},
		},
		{
			name: "activity",
			path: activityKey.Path,
			misplaced: &temporalessv1.ActivityRecord{
				SchemaVersion: ActivityRecordSchemaVersion,
				Key:           otherActivityKey.Proto(),
			},
			wrong: &temporalessv1.ActivityRecord{
				SchemaVersion: WorkflowRecordSchemaVersion,
				Key:           activityKey.Proto(),
			},
			get: func(ctx context.Context, store *OpenDALStore) (bool, error) {
				_, found, err := store.GetActivity(ctx, activityKey)
				return found, err
			},
		},
		{
			name: "timer",
			path: timerKey.Path,
			misplaced: &temporalessv1.TimerRecord{
				SchemaVersion: TimerRecordSchemaVersion,
				Key:           otherTimerKey.Proto(),
			},
			wrong: &temporalessv1.TimerRecord{
				SchemaVersion: EventRecordSchemaVersion,
				Key:           timerKey.Proto(),
			},
			get: func(ctx context.Context, store *OpenDALStore) (bool, error) {
				_, found, err := store.GetTimer(ctx, timerKey)
				return found, err
			},
		},
		{
			name: "event",
			path: eventKey.Path,
			misplaced: &temporalessv1.EventRecord{
				SchemaVersion: EventRecordSchemaVersion,
				Key:           otherEventKey.Proto(),
			},
			wrong: &temporalessv1.EventRecord{
				SchemaVersion: TimerRecordSchemaVersion,
				Key:           eventKey.Proto(),
			},
			get: func(ctx context.Context, store *OpenDALStore) (bool, error) {
				_, found, err := store.GetEvent(ctx, eventKey)
				return found, err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, payload := range []struct {
				name   string
				record proto.Message
			}{
				{name: "misplaced key", record: test.misplaced},
				{name: "wrong schema", record: test.wrong},
			} {
				t.Run(payload.name, func(t *testing.T) {
					store, operator := newPointValidationStore(t)
					path, err := test.path()
					if err != nil {
						t.Fatal(err)
					}
					data, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload.record)
					if err != nil {
						t.Fatal(err)
					}
					writeDueTimerTestBytes(t, operator, path, data)

					found, err := test.get(context.Background(), store)
					if found || !errors.Is(err, ErrCorruptRecord) {
						t.Fatalf("found=%v err=%v, want false/ErrCorruptRecord", found, err)
					}
				})
			}
		})
	}
}

func TestDeleteTimerCannotFollowCorruptEmbeddedKey(t *testing.T) {
	ctx := context.Background()
	store, operator := newPointValidationStore(t)
	now := timestamppb.Now()
	requested := NewTimerKey("workflow-a", "run-a", "timer-a")
	other := NewTimerKey("workflow-b", "run-b", "timer-b")
	otherRecord := &temporalessv1.TimerRecord{
		SchemaVersion: TimerRecordSchemaVersion,
		Key:           other.Proto(),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        now,
	}
	if err := store.PutTimer(ctx, otherRecord); err != nil {
		t.Fatal(err)
	}
	otherLedger, err := dueEntryPath(other)
	if err != nil {
		t.Fatal(err)
	}
	requestedPath, err := requested.Path()
	if err != nil {
		t.Fatal(err)
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(otherRecord)
	if err != nil {
		t.Fatal(err)
	}
	writeDueTimerTestBytes(t, operator, requestedPath, data)

	deleted, err := store.DeleteTimer(ctx, requested)
	if err != nil || !deleted {
		t.Fatalf("DeleteTimer: deleted=%v err=%v", deleted, err)
	}
	if _, found, err := store.GetTimer(ctx, other); err != nil || !found {
		t.Fatalf("other timer was redirected: found=%v err=%v", found, err)
	}
	if exists, err := operator.IsExist(otherLedger); err != nil || !exists {
		t.Fatalf("other timer ledger was redirected: exists=%v err=%v", exists, err)
	}
}

func TestOpenDALStorePutRejectsWrongSchemasBeforeWrite(t *testing.T) {
	ctx := context.Background()
	workflowKey := NewWorkflowKey("workflow", "run")
	activityKey := NewActivityKey("workflow", "run", "activity")
	timerKey := NewTimerKey("workflow", "run", "timer")
	eventKey := NewEventKey("workflow", "run", "event")

	tests := []struct {
		name string
		path func() (string, error)
		put  func(context.Context, *OpenDALStore) error
	}{
		{
			name: "workflow",
			path: workflowKey.Path,
			put: func(ctx context.Context, store *OpenDALStore) error {
				return store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
					SchemaVersion: ActivityRecordSchemaVersion,
					Key:           workflowKey.Proto(),
				})
			},
		},
		{
			name: "activity",
			path: activityKey.Path,
			put: func(ctx context.Context, store *OpenDALStore) error {
				return store.PutActivity(ctx, &temporalessv1.ActivityRecord{
					SchemaVersion: WorkflowRecordSchemaVersion,
					Key:           activityKey.Proto(),
				})
			},
		},
		{
			name: "timer",
			path: timerKey.Path,
			put: func(ctx context.Context, store *OpenDALStore) error {
				return store.PutTimer(ctx, &temporalessv1.TimerRecord{
					SchemaVersion: EventRecordSchemaVersion,
					Key:           timerKey.Proto(),
				})
			},
		},
		{
			name: "event",
			path: eventKey.Path,
			put: func(ctx context.Context, store *OpenDALStore) error {
				return store.PutEvent(ctx, &temporalessv1.EventRecord{
					SchemaVersion: TimerRecordSchemaVersion,
					Key:           eventKey.Proto(),
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, operator := newPointValidationStore(t)
			if err := test.put(ctx, store); !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("err=%v, want ErrCorruptRecord", err)
			}
			path, err := test.path()
			if err != nil {
				t.Fatal(err)
			}
			if exists, err := operator.IsExist(path); err != nil || exists {
				t.Fatalf("invalid write touched authoritative object: exists=%v err=%v", exists, err)
			}
		})
	}
}

func TestOpenDALStorePutWorkflowValidatesPointerTimesBeforeWrite(t *testing.T) {
	ctx := context.Background()
	key := NewWorkflowKey("workflow", "run")
	invalidTimestamp := &timestamppb.Timestamp{Seconds: 253402300800}

	tests := []struct {
		name   string
		record *temporalessv1.WorkflowRecord
	}{
		{
			name: "invalid run order time",
			record: &temporalessv1.WorkflowRecord{
				SchemaVersion: WorkflowRecordSchemaVersion,
				Key:           key.Proto(),
				Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
				RunOrderTime:  invalidTimestamp,
			},
		},
		{
			name: "invalid lifecycle time",
			record: &temporalessv1.WorkflowRecord{
				SchemaVersion: WorkflowRecordSchemaVersion,
				Key:           key.Proto(),
				Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
				CompletedAt:   invalidTimestamp,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, operator := newPointValidationStore(t)
			if err := store.PutWorkflow(ctx, test.record); err == nil {
				t.Fatal("expected invalid workflow timestamp error")
			}
			path, err := key.Path()
			if err != nil {
				t.Fatal(err)
			}
			if exists, err := operator.IsExist(path); err != nil || exists {
				t.Fatalf("invalid workflow write committed: exists=%v err=%v", exists, err)
			}
		})
	}
}

func newPointValidationStore(t *testing.T) (*OpenDALStore, *opendal.Operator) {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return NewOpenDALStore(operator), operator
}
