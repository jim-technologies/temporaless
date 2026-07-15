package connectstore

import (
	"context"
	"errors"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type corruptClientPointStore struct {
	storage.Store
	workflow     *temporalessv1.WorkflowRecord
	activity     *temporalessv1.ActivityRecord
	timer        *temporalessv1.TimerRecord
	event        *temporalessv1.EventRecord
	pointer      *temporalessv1.LatestWorkflowRunPointer
	hideWorkflow bool
	activities   []*temporalessv1.ActivityRecord
	timers       []*temporalessv1.TimerRecord
	events       []*temporalessv1.EventRecord
	due          []storage.DueTimer
}

func (store *corruptClientPointStore) GetWorkflow(
	ctx context.Context,
	key storage.WorkflowKey,
) (*temporalessv1.WorkflowRecord, bool, error) {
	if store.hideWorkflow {
		return nil, false, nil
	}
	if store.workflow != nil {
		return store.workflow, true, nil
	}
	return store.Store.GetWorkflow(ctx, key)
}

func (store *corruptClientPointStore) GetActivity(
	ctx context.Context,
	key storage.ActivityKey,
) (*temporalessv1.ActivityRecord, bool, error) {
	if store.activity != nil {
		return store.activity, true, nil
	}
	return store.Store.GetActivity(ctx, key)
}

func (store *corruptClientPointStore) GetTimer(
	ctx context.Context,
	key storage.TimerKey,
) (*temporalessv1.TimerRecord, bool, error) {
	if store.timer != nil {
		return store.timer, true, nil
	}
	return store.Store.GetTimer(ctx, key)
}

func (store *corruptClientPointStore) GetEvent(
	ctx context.Context,
	key storage.EventKey,
) (*temporalessv1.EventRecord, bool, error) {
	if store.event != nil {
		return store.event, true, nil
	}
	return store.Store.GetEvent(ctx, key)
}

func (store *corruptClientPointStore) GetLatestWorkflowRun(
	ctx context.Context,
	namespace string,
	workflowID string,
) (*temporalessv1.LatestWorkflowRunPointer, bool, error) {
	if store.pointer != nil {
		return store.pointer, true, nil
	}
	return store.Store.GetLatestWorkflowRun(ctx, namespace, workflowID)
}

func (store *corruptClientPointStore) ListActivities(context.Context, storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	if store.activities != nil {
		return store.activities, nil
	}
	return nil, nil
}

func (store *corruptClientPointStore) ListTimers(context.Context, storage.WorkflowKey, temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	if store.timers != nil {
		return store.timers, nil
	}
	return nil, nil
}

func (store *corruptClientPointStore) ListEvents(context.Context, storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	if store.events != nil {
		return store.events, nil
	}
	return nil, nil
}

func (store *corruptClientPointStore) DueTimers(context.Context, string, time.Time) ([]storage.DueTimer, error) {
	return store.due, nil
}

func TestClientStoreRejectsMisplacedPointPayloads(t *testing.T) {
	ctx := context.Background()
	requestedWorkflow := storage.NewWorkflowKey("workflow", "run")
	requestedActivity := storage.NewActivityKey("workflow", "run", "activity")
	requestedTimer := storage.NewTimerKey("workflow", "run", "timer")
	requestedEvent := storage.NewEventKey("workflow", "run", "event")

	tests := []struct {
		name   string
		store  *corruptClientPointStore
		invoke func(*ClientStore) error
	}{
		{
			name: "workflow",
			store: &corruptClientPointStore{workflow: &temporalessv1.WorkflowRecord{
				SchemaVersion: storage.WorkflowRecordSchemaVersion,
				Key:           storage.NewWorkflowKey("other", "other-run").Proto(),
			}},
			invoke: func(client *ClientStore) error {
				_, _, err := client.GetWorkflow(ctx, requestedWorkflow)
				return err
			},
		},
		{
			name: "activity",
			store: &corruptClientPointStore{activity: &temporalessv1.ActivityRecord{
				SchemaVersion: storage.ActivityRecordSchemaVersion,
				Key:           storage.NewActivityKey("other", "other-run", "other").Proto(),
			}},
			invoke: func(client *ClientStore) error {
				_, _, err := client.GetActivity(ctx, requestedActivity)
				return err
			},
		},
		{
			name: "timer",
			store: &corruptClientPointStore{timer: &temporalessv1.TimerRecord{
				SchemaVersion: storage.TimerRecordSchemaVersion,
				Key:           storage.NewTimerKey("other", "other-run", "other").Proto(),
			}},
			invoke: func(client *ClientStore) error {
				_, _, err := client.GetTimer(ctx, requestedTimer)
				return err
			},
		},
		{
			name: "event",
			store: &corruptClientPointStore{event: &temporalessv1.EventRecord{
				SchemaVersion: storage.EventRecordSchemaVersion,
				Key:           storage.NewEventKey("other", "other-run", "other").Proto(),
			}},
			invoke: func(client *ClientStore) error {
				_, _, err := client.GetEvent(ctx, requestedEvent)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.store.Store = newTestStore(t)
			err := test.invoke(NewClientStore(NewHandler(test.store)))
			if !errors.Is(err, storage.ErrCorruptRecord) {
				t.Fatalf("err=%v, want ErrCorruptRecord", err)
			}
		})
	}
}

func TestClientStoreValidatesLatestReferenceAndLists(t *testing.T) {
	ctx := context.Background()
	requested := storage.NewWorkflowKey("workflow", "run")
	other := storage.NewWorkflowKey("other", "other-run")

	t.Run("dangling latest pointer", func(t *testing.T) {
		backend := newTestStore(t)
		completedAt := timestamppb.New(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))
		if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
			SchemaVersion: storage.WorkflowRecordSchemaVersion,
			Key:           requested.Proto(),
			Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			CreatedAt:     completedAt,
			CompletedAt:   completedAt,
		}); err != nil {
			t.Fatal(err)
		}
		pointer, found, err := backend.GetLatestWorkflowRun(ctx, requested.Namespace, requested.WorkflowID)
		if err != nil || !found {
			t.Fatalf("pointer: found=%v err=%v", found, err)
		}
		corrupt := &corruptClientPointStore{Store: backend, pointer: pointer, hideWorkflow: true}
		_, found, err = NewClientStore(NewHandler(corrupt)).GetLatestWorkflowRun(ctx, requested.Namespace, requested.WorkflowID)
		if found || err != nil {
			t.Fatalf("found=%v err=%v, want not found", found, err)
		}
	})

	tests := []struct {
		name   string
		store  *corruptClientPointStore
		invoke func(*ClientStore) error
	}{
		{
			name: "activities",
			store: &corruptClientPointStore{activities: []*temporalessv1.ActivityRecord{{
				SchemaVersion: storage.ActivityRecordSchemaVersion,
				Key:           storage.NewActivityKey(other.WorkflowID, other.RunID, "activity").Proto(),
			}}},
			invoke: func(client *ClientStore) error {
				_, err := client.ListActivities(ctx, requested)
				return err
			},
		},
		{
			name: "timers",
			store: &corruptClientPointStore{timers: []*temporalessv1.TimerRecord{{
				SchemaVersion: storage.TimerRecordSchemaVersion,
				Key:           storage.NewTimerKey(other.WorkflowID, other.RunID, "timer").Proto(),
			}}},
			invoke: func(client *ClientStore) error {
				_, err := client.ListTimers(ctx, requested, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
				return err
			},
		},
		{
			name: "events",
			store: &corruptClientPointStore{events: []*temporalessv1.EventRecord{{
				SchemaVersion: storage.EventRecordSchemaVersion,
				Key:           storage.NewEventKey(other.WorkflowID, other.RunID, "event").Proto(),
			}}},
			invoke: func(client *ClientStore) error {
				_, err := client.ListEvents(ctx, requested)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.store.Store = newTestStore(t)
			if err := test.invoke(NewClientStore(NewHandler(test.store))); !errors.Is(err, storage.ErrCorruptRecord) {
				t.Fatalf("err=%v, want ErrCorruptRecord", err)
			}
		})
	}
}

type corruptClientClaimStore struct {
	record *temporalessv1.ClaimRecord
}

func (store *corruptClientClaimStore) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return storage.CreateOnlyClaims, nil
}

func (store *corruptClientClaimStore) GetClaim(context.Context, storage.ClaimKey) (*temporalessv1.ClaimRecord, bool, error) {
	return store.record, true, nil
}

func (*corruptClientClaimStore) TryCreateClaim(context.Context, *temporalessv1.ClaimRecord) (bool, error) {
	return false, nil
}

func (*corruptClientClaimStore) DeleteClaim(context.Context, storage.ClaimKey) (bool, error) {
	return false, nil
}

func TestClientStoreRejectsMisplacedClaimPayload(t *testing.T) {
	requested := storage.NewClaimKey("workflow", "run", "claim")
	claims := &corruptClientClaimStore{record: &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           storage.NewClaimKey("other", "other-run", "other").Proto(),
	}}
	client := NewClientStore(NewHandlerWithClaims(newTestStore(t), claims))
	_, found, err := client.GetClaim(context.Background(), requested)
	if found || !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("found=%v err=%v", found, err)
	}
}

func TestClientStoreRejectsMisplacedDueTimerPayload(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	key := storage.NewTimerKey("workflow", "run", "timer")
	otherKey := storage.NewTimerKey("other", "other-run", "other-timer")
	workflowKey := storage.NewWorkflowKey(key.WorkflowID, key.RunID)
	backend := newTestStore(t)
	corrupt := &corruptClientPointStore{
		Store: backend,
		due: []storage.DueTimer{{
			Key: key,
			Record: &temporalessv1.TimerRecord{
				SchemaVersion: storage.TimerRecordSchemaVersion,
				Key:           otherKey.Proto(),
				Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
				FireAt:        timestamppb.New(now),
			},
			Workflow: &temporalessv1.WorkflowRecord{
				SchemaVersion: storage.WorkflowRecordSchemaVersion,
				Key:           workflowKey.Proto(),
				Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
			},
		}},
	}
	_, err := NewClientStore(NewHandler(corrupt)).DueTimers(context.Background(), "default", now)
	if !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("err=%v, want ErrCorruptRecord", err)
	}
}
