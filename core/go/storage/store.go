package storage

import (
	"context"
	"errors"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

// ErrInvalidQuery marks caller-supplied query options that an optional query
// adapter cannot accept. Transport adapters map it to INVALID_ARGUMENT.
var ErrInvalidQuery = errors.New("invalid query")

// ErrCorruptRecord marks a protobuf payload whose schema or embedded identity
// does not match the authoritative point at which it was read. Callers may use
// errors.Is to isolate one damaged object without treating a backend outage as
// an empty result.
var ErrCorruptRecord = errors.New("corrupt storage record")

// ErrStaleLatestPointer marks a well-formed derived pointer whose referenced
// workflow has already advanced to different metadata. This is an expected
// observation between the authoritative WorkflowRecord write and the later
// best-effort pointer write, not evidence of authoritative data corruption.
var ErrStaleLatestPointer = errors.New("stale latest workflow run pointer")

// DueTimer pairs a SCHEDULED timer with the workflow that owns it. Returned
// by [Store.DueTimers] so callers can re-invoke the parent workflow when its
// sleep is up. The deterministic due-ledger object carries the exact prepared
// TimerRecord and is written before the canonical run record. A scanner repairs
// an interrupted write first and emits the wake only after a later scan observes
// both copies in agreement.
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
	// GetLatestWorkflowRun reads the derived point pointer for one workflow ID.
	// The pointer is a scheduler optimization, not an authoritative run record.
	GetLatestWorkflowRun(ctx context.Context, namespace string, workflowID string) (*temporalessv1.LatestWorkflowRunPointer, bool, error)
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
	// the runtime to release workflow-execution, activity, and concurrency-key
	// claims at their durable/orderly boundaries.
	DeleteClaim(context.Context, ClaimKey) (bool, error)
}

// ClaimRunStore is the optional, run-scoped extension used by DeleteRun.
// It deliberately exposes no cross-run claim search or filtering.
type ClaimRunStore interface {
	ClaimStore
	ListClaims(context.Context, WorkflowKey) ([]*temporalessv1.ClaimRecord, error)
}

type Store interface {
	ActivityStore
	EventStore
	TimerStore
	WorkflowStore

	// DueTimers returns SCHEDULED timer wakes under the given namespace whose
	// fire_at <= now and whose parent workflow is still IN_PROGRESS. Returned
	// records have matching prepared-ledger and canonical copies.
	DueTimers(ctx context.Context, namespace string, now time.Time) ([]DueTimer, error)
}

// WorkflowQueryStore is the narrow cross-run candidate source used by
// inspectors and retention. Implementations must be derived query adapters;
// the authoritative bucket Store deliberately does not implement it.
type WorkflowQueryStore interface {
	ListWorkflows(context.Context, *temporalessv1.ListWorkflowsRequest) (*temporalessv1.ListWorkflowsResponse, error)
}

// QueryStore is the optional cross-run query and retention surface. It mirrors
// RecordQueryService with generated protobuf requests and responses. SQL or
// another rebuildable index belongs in an adapter, never in the core bucket
// store. The Query suffixes avoid colliding with Store's run-scoped methods.
type QueryStore interface {
	WorkflowQueryStore
	ListActivitiesQuery(context.Context, *temporalessv1.RecordQueryServiceListActivitiesRequest) (*temporalessv1.RecordQueryServiceListActivitiesResponse, error)
	Sweep(context.Context, *temporalessv1.SweepRequest) (*temporalessv1.SweepResponse, error)
	DueTimersQuery(context.Context, *temporalessv1.RecordQueryServiceDueTimersRequest) (*temporalessv1.RecordQueryServiceDueTimersResponse, error)
}
