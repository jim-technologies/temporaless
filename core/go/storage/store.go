package storage

import (
	"context"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

// DueTimer pairs a SCHEDULED timer with the workflow that owns it. Returned
// by [Store.DueTimers] so callers can re-invoke the parent workflow when its
// sleep is up.
type DueTimer struct {
	Key      TimerKey
	Record   *temporalessv1.TimerRecord
	Workflow *temporalessv1.WorkflowRecord
}

type ActivityStore interface {
	GetActivity(context.Context, ActivityKey) (*temporalessv1.ActivityRecord, bool, error)
	PutActivity(context.Context, *temporalessv1.ActivityRecord) error
	ListActivities(context.Context, WorkflowKey) ([]*temporalessv1.ActivityRecord, error)
	DeleteActivity(context.Context, ActivityKey) (bool, error)
}

type WorkflowStore interface {
	GetWorkflow(context.Context, WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error)
	PutWorkflow(context.Context, *temporalessv1.WorkflowRecord) error
	// ListWorkflows returns workflows under the given namespace + workflow_id.
	// Empty namespace lists across all namespaces. Empty workflowID lists
	// across all workflow_ids in the namespace(s). WORKFLOW_STATUS_UNSPECIFIED
	// matches all statuses.
	ListWorkflows(ctx context.Context, namespace string, workflowID string, status temporalessv1.WorkflowStatus) ([]*temporalessv1.WorkflowRecord, error)
	DeleteWorkflow(context.Context, WorkflowKey) (bool, error)
}

type TimerStore interface {
	GetTimer(context.Context, TimerKey) (*temporalessv1.TimerRecord, bool, error)
	PutTimer(context.Context, *temporalessv1.TimerRecord) error
	// ListTimers returns timer records under the given workflow run.
	// TIMER_STATUS_UNSPECIFIED matches all statuses.
	ListTimers(ctx context.Context, key WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error)
	DeleteTimer(context.Context, TimerKey) (bool, error)
}

type EventStore interface {
	GetEvent(context.Context, EventKey) (*temporalessv1.EventRecord, bool, error)
	PutEvent(context.Context, *temporalessv1.EventRecord) error
	ListEvents(context.Context, WorkflowKey) ([]*temporalessv1.EventRecord, error)
	DeleteEvent(context.Context, EventKey) (bool, error)
}

type ClaimCapability = temporalessv1.ClaimCapability

const (
	NoClaims         = temporalessv1.ClaimCapability_CLAIM_CAPABILITY_NO_CLAIMS
	CreateOnlyClaims = temporalessv1.ClaimCapability_CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS
	CASClaims        = temporalessv1.ClaimCapability_CLAIM_CAPABILITY_CAS_CLAIMS
)

type ClaimStore interface {
	ClaimCapability(context.Context) (ClaimCapability, error)
	GetClaim(context.Context, ClaimKey) (*temporalessv1.ClaimRecord, bool, error)
	TryCreateClaim(context.Context, *temporalessv1.ClaimRecord) (bool, error)
	// DeleteClaim idempotently releases a held claim. Returns true when the
	// claim existed and was removed, false when it was already absent. Used by
	// the runtime to release concurrency-key slots when a workflow reaches a
	// terminal status or returns a pending error.
	DeleteClaim(context.Context, ClaimKey) (bool, error)
}

type Store interface {
	ActivityStore
	EventStore
	TimerStore
	WorkflowStore

	// Sweep deletes every COMPLETED workflow run under the given namespace
	// (empty = all namespaces) whose completed_at is older than now-maxAge.
	// Activities, timers, and events under each swept run are deleted before
	// the workflow record itself. Returns the number of runs deleted.
	Sweep(ctx context.Context, namespace string, now time.Time, maxAge time.Duration) (uint32, error)

	// DueTimers returns SCHEDULED timer records under the given namespace
	// whose fire_at <= now and whose parent workflow is still IN_PROGRESS.
	DueTimers(ctx context.Context, namespace string, now time.Time) ([]DueTimer, error)
}
