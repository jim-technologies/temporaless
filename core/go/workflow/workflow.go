package workflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"buf.build/go/protovalidate"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrActivityConflict = errors.New("activity record conflicts with requested activity")
var ErrActivityFailed = errors.New("activity failed")
var ErrClaimBusy = errors.New("claim is busy")
var ErrClaimRelease = errors.New("claim release failed")
var ErrEventPending = errors.New("event is pending")
var ErrTimerConflict = errors.New("timer record conflicts with requested timer")
var ErrTimerPending = errors.New("timer is pending")
var ErrWorkflowConflict = errors.New("workflow record conflicts with requested workflow")
var ErrWorkflowDependencyPending = errors.New("workflow dependency has not completed")
var ErrWorkflowDependencyFailed = errors.New("workflow dependency ended in a non-COMPLETED terminal state")

const DefaultClaimLeaseDuration = time.Duration(temporalessv1.Default_RuntimeDefaults_ClaimLeaseDurationSeconds) * time.Second

// ActivityRetryTimerIDPrefix marks timer records owned by the runtime's
// durable retry path. Sourced from the proto-declared default on
// `ReservedNames.activity_retry_timer_id_prefix` so the SDK and the proto
// contract can't drift. User code passing this prefix to workflow.Sleep is
// rejected so framework-managed retry timers don't collide with user timers.
var ActivityRetryTimerIDPrefix = temporalessv1.Default_ReservedNames_ActivityRetryTimerIdPrefix

// ActivityClaimIDPrefix namespaces claims that serialize one activity ID.
// Sourced from ReservedNames so Go and Python persist identical claim keys.
var ActivityClaimIDPrefix = temporalessv1.Default_ReservedNames_ActivityClaimIdPrefix

// WorkflowExecutionClaimID is the deterministic claim_id used to serialize
// live invocations of one workflow run. The workflow_id and run_id live in the
// surrounding ClaimKey. Sourced from ReservedNames so SDKs cannot drift.
var WorkflowExecutionClaimID = temporalessv1.Default_ReservedNames_WorkflowExecutionClaimId

// activityRetryTimerID returns the deterministic timer_id used to pair an
// ActivityRecord with its durable retry timer. Stable per activity_id; later
// retries overwrite the record with a new fire_at.
func activityRetryTimerID(activityID string) string {
	return ActivityRetryTimerIDPrefix + activityID
}

type WorkflowFunc[Req proto.Message, Resp proto.Message] func(context.Context, Req) (Resp, error)
type ActivityFunc[Req proto.Message, Resp proto.Message] func(context.Context, Req) (Resp, error)

type Options = temporalessv1.WorkflowOptions
type ActivityOptions = temporalessv1.ActivityOptions
type RetryPolicy = temporalessv1.RetryPolicy

type ActivityError struct {
	Code    string
	Message string
	// RetryAfter, when > 0, overrides the retry policy's computed interval
	// for the next attempt: the planner uses max(computed, RetryAfter). Set
	// this from a vendor's HTTP `Retry-After` header, a Slack
	// `Retry-After-In` field, or an OpenAI `x-ratelimit-reset` timestamp.
	RetryAfter time.Duration
	Cause      error
}

func (err *ActivityError) Error() string {
	if err.Code != "" {
		return fmt.Sprintf("activity error [%s]: %s", err.Code, err.Message)
	}
	return fmt.Sprintf("activity error: %s", err.Message)
}

func (err *ActivityError) Unwrap() error {
	if err.Cause != nil {
		return err.Cause
	}
	return ErrActivityFailed
}

func NewActivityError(code string, message string, cause error) *ActivityError {
	return &ActivityError{Code: code, Message: message, Cause: cause}
}

// NewRetryableActivityError attaches a vendor-supplied retry-after duration
// so the retry planner waits at least that long before the next attempt.
// Use this when the vendor returns a 429 with `Retry-After: N` or any
// equivalent header.
func NewRetryableActivityError(code, message string, retryAfter time.Duration, cause error) *ActivityError {
	return &ActivityError{Code: code, Message: message, RetryAfter: retryAfter, Cause: cause}
}

type Workflow struct {
	store           storage.Store
	claimStore      storage.ClaimStore
	claimCapability storage.ClaimCapability
	workflowID      string
	runID           string
	codeVersion     string
	claimOwner      string
}

type workflowContextKey struct{}

type annotationsKey struct{}

type annotationsBag struct {
	mu   sync.Mutex
	data map[string]string
}

func newAnnotationsBag() *annotationsBag {
	return &annotationsBag{data: map[string]string{}}
}

func (a *annotationsBag) set(key, value string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.data[key] = value
}

func (a *annotationsBag) snapshot() map[string]string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.data) == 0 {
		return nil
	}
	out := make(map[string]string, len(a.data))
	for k, v := range a.data {
		out[k] = v
	}
	return out
}

// Annotate attaches a key/value pair to the running activity record (when called
// from inside an activity) or to the running workflow record (when called from
// the workflow body between activity calls). Annotations are persisted on the
// stored record and survive replay.
func Annotate(ctx context.Context, key string, value string) {
	if bag, ok := ctx.Value(annotationsKey{}).(*annotationsBag); ok && bag != nil {
		bag.set(key, value)
	}
}

func (w *Workflow) WorkflowID() string  { return w.workflowID }
func (w *Workflow) RunID() string       { return w.runID }
func (w *Workflow) CodeVersion() string { return w.codeVersion }

// Store returns the Store this workflow is replaying against. Exposed so
// adapter helpers (e.g. dependencies.WaitForWorkflow) can read records
// without reaching into private state.
func (w *Workflow) Store() storage.Store { return w.store }

type TimerPendingError struct {
	TimerID string
	WakeAt  time.Time
}

func (err *TimerPendingError) Error() string {
	return fmt.Sprintf("timer %q is pending until %s", err.TimerID, err.WakeAt.UTC().Format(time.RFC3339Nano))
}

func (err *TimerPendingError) Unwrap() error {
	return ErrTimerPending
}

type EventPendingError struct {
	EventID string
}

func (err *EventPendingError) Error() string {
	return fmt.Sprintf("event %q is pending", err.EventID)
}

func (err *EventPendingError) Unwrap() error {
	return ErrEventPending
}

type ClaimBusyError struct {
	ClaimID        string
	OwnerID        string
	LeaseExpiresAt time.Time
	Capability     storage.ClaimCapability
}

func (err *ClaimBusyError) Error() string {
	if err.LeaseExpiresAt.IsZero() {
		return fmt.Sprintf("claim %q is busy", err.ClaimID)
	}
	return fmt.Sprintf(
		"claim %q is busy (recorded lease expiry %s)",
		err.ClaimID,
		err.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
	)
}

func (err *ClaimBusyError) Unwrap() error {
	return ErrClaimBusy
}

// WorkflowDependencyPendingError is raised when a workflow body waits on
// another workflow that hasn't completed yet. Like EventPendingError, this
// leaves the calling workflow IN_PROGRESS so a scanner / re-invoke can resume
// it later.
type WorkflowDependencyPendingError struct {
	WorkflowID string
	RunID      string
}

func (err *WorkflowDependencyPendingError) Error() string {
	return fmt.Sprintf("workflow %q/%q has not completed", err.WorkflowID, err.RunID)
}

func (err *WorkflowDependencyPendingError) Unwrap() error {
	return ErrWorkflowDependencyPending
}

// WorkflowDependencyFailedError is raised when a workflow body waits on
// another workflow that ended in a non-COMPLETED terminal status. The
// dependency is unrecoverable without operator action — propagating as a
// typed error means downstream workflows fail loudly rather than waiting
// forever.
type WorkflowDependencyFailedError struct {
	WorkflowID string
	RunID      string
	Status     int32
}

func (err *WorkflowDependencyFailedError) Error() string {
	return fmt.Sprintf(
		"workflow %q/%q dependency failed (status=%d)",
		err.WorkflowID,
		err.RunID,
		err.Status,
	)
}

func (err *WorkflowDependencyFailedError) Unwrap() error {
	return ErrWorkflowDependencyFailed
}

func Run[Req proto.Message, Resp proto.Message](
	ctx context.Context,
	store storage.Store,
	options *Options,
	claimStore storage.ClaimStore,
	input Req,
	newResult func() Resp,
	execute WorkflowFunc[Req, Resp],
) (returnResult Resp, returnErr error) {
	var zero Resp
	if store == nil {
		return zero, fmt.Errorf("store is required")
	}
	if isNilMessage(input) {
		return zero, fmt.Errorf("workflow input is required")
	}
	if newResult == nil {
		return zero, fmt.Errorf("workflow result constructor is required")
	}
	if execute == nil {
		return zero, fmt.Errorf("workflow executor is required")
	}
	runOptions, err := normalizedWorkflowOptions(options)
	if err != nil {
		return zero, err
	}
	if runOptions.GetConcurrencyKey() != "" && claimStore == nil {
		return zero, fmt.Errorf("claim store is required when concurrency_key is set")
	}
	if runOptions.GetClaimOwnerId() != "" && claimStore == nil {
		return zero, fmt.Errorf("claim store is required when claim_owner_id is set")
	}

	resultTemplate := newResult()
	if isNilMessage(resultTemplate) {
		return zero, fmt.Errorf("workflow result constructor returned nil")
	}

	workflowType := messagePairType("workflow", input, resultTemplate)
	key := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: runOptions.GetWorkflowId(),
		RunID:      runOptions.GetRunId(),
	}

	// Read workflow state from the authoritative store before constructing the
	// run cache. In particular, a failed execution-claim race must not consult a
	// negative cache populated before the winning invocation wrote its result.
	rawStore := store
	record, found, err := rawStore.GetWorkflow(ctx, key)
	if err != nil {
		return zero, err
	}

	inspectRecord := func(record *temporalessv1.WorkflowRecord, found bool) (Resp, *timestamppb.Timestamp, bool, error) {
		if !found {
			return zero, nil, false, nil
		}
		switch record.GetStatus() {
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			result, replayErr := replayWorkflowRecord(record, workflowType, runOptions.GetCodeVersion(), newResult)
			return result, nil, true, replayErr
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS:
			if identityErr := assertWorkflowIdentity(record, workflowType, runOptions.GetCodeVersion()); identityErr != nil {
				return zero, nil, false, identityErr
			}
			return zero, record.GetCreatedAt(), false, nil
		default:
			return zero, nil, false, fmt.Errorf("%w: stored workflow has unknown status", ErrWorkflowConflict)
		}
	}

	replayed, createdAt, terminal, err := inspectRecord(record, found)
	if terminal || err != nil {
		return replayed, err
	}

	claimCapability := temporalessv1.ClaimCapability_CLAIM_CAPABILITY_UNSPECIFIED
	claimOption := ""
	if runOptions.GetConcurrencyKey() != "" {
		claimOption = "concurrency_key"
	} else if runOptions.GetClaimOwnerId() != "" {
		claimOption = "claim_owner_id"
	}
	if claimOption != "" {
		claimCapability, err = claimStore.ClaimCapability(ctx)
		if err != nil {
			return zero, err
		}
		if !supportsCreateOnlyClaims(claimCapability) {
			return zero, &ClaimCapabilityError{
				Capability: claimCapability,
				Option:     claimOption,
			}
		}
	}

	// A caller-provided claim_owner_id opts this run into storage-backed
	// single-flight execution. Any existing claim is busy, including one with
	// the same owner: two live requests commonly reuse one worker identity and
	// must never enter the same workflow body together. Normal resume works
	// because every return path below releases the claim.
	if runOptions.GetClaimOwnerId() != "" {
		workflowClaimKey := storage.ClaimKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: runOptions.GetWorkflowId(),
			RunID:      runOptions.GetRunId(),
			ClaimID:    WorkflowExecutionClaimID,
		}
		acquired := false
		for attempt := 0; attempt < 2; attempt++ {
			now := time.Now().UTC()
			created, createErr := claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
				SchemaVersion:  storage.ClaimRecordSchemaVersion,
				Key:            workflowClaimKey.Proto(),
				OwnerId:        runOptions.GetClaimOwnerId(),
				ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
				ResourceId:     runOptions.GetWorkflowId(),
				CodeVersion:    runOptions.GetCodeVersion(),
				LeaseExpiresAt: timestamppb.New(now.Add(DefaultClaimLeaseDuration)),
				CreatedAt:      timestamppb.New(now),
				HeartbeatAt:    timestamppb.New(now),
			})
			if createErr != nil {
				return zero, createErr
			}
			if created {
				acquired = true
				break
			}

			// The winner may have completed between our first workflow read and
			// failed claim creation. Re-read the raw store so a terminal result
			// wins over a stale/busy claim and can be replayed immediately.
			fresh, freshFound, freshErr := rawStore.GetWorkflow(ctx, key)
			if freshErr != nil {
				return zero, freshErr
			}
			freshReplay, _, freshTerminal, inspectErr := inspectRecord(fresh, freshFound)
			if freshTerminal || inspectErr != nil {
				return freshReplay, inspectErr
			}

			existing, claimFound, getErr := claimStore.GetClaim(ctx, workflowClaimKey)
			if getErr != nil {
				return zero, getErr
			}
			// A release can race the failed create. Retry once when the claim
			// disappeared; otherwise report the current holder as busy.
			if claimFound || attempt == 1 {
				busy := &ClaimBusyError{
					ClaimID:    workflowClaimKey.ClaimID,
					Capability: claimCapability,
				}
				if claimFound {
					busy.OwnerID = existing.GetOwnerId()
					if existing.GetLeaseExpiresAt() != nil {
						busy.LeaseExpiresAt = existing.GetLeaseExpiresAt().AsTime()
					}
				}
				return zero, busy
			}
		}
		if !acquired {
			return zero, fmt.Errorf("failed to acquire workflow execution claim")
		}

		defer func() {
			// Release with a fresh context so request cancellation does not leak
			// a create-only claim during an otherwise orderly return. Preserve
			// request-scoped auth/routing values for remote claim stores.
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer releaseCancel()
			if _, releaseErr := claimStore.DeleteClaim(releaseCtx, workflowClaimKey); releaseErr != nil {
				returnResult = zero
				returnErr = errors.Join(
					returnErr,
					fmt.Errorf("%w: workflow execution claim: %w", ErrClaimRelease, releaseErr),
				)
			}
		}()

		// State may have changed between the initial read and acquisition (for
		// example, a prior holder completed and released). Refresh before any
		// cache/prefetch or workflow-body execution.
		record, found, err = rawStore.GetWorkflow(ctx, key)
		if err != nil {
			return zero, err
		}
		replayed, createdAt, terminal, err = inspectRecord(record, found)
		if terminal || err != nil {
			return replayed, err
		}
	}

	// Substitute the user-provided store with a run-scoped cache. The cache is
	// write-through for the underlying store and serves Get-by-key reads from
	// memory after prefetch — turning N round-trips per replay into one List
	// per record kind. Out-of-scope reads (e.g. cross-pipeline dependencies)
	// pass straight through. See cache.go for the full contract.
	cachedStore := newRunScopedCache(rawStore, key)
	store = cachedStore
	if createdAt != nil {
		// Replay: prefetch activities, timers, events in parallel so the body's
		// subsequent Get calls hit memory instead of issuing N individual
		// round-trips against the underlying store.
		if err := cachedStore.prefetch(ctx); err != nil {
			return zero, err
		}
	}

	// Pre-emptive cluster-wide concurrency cap. Acquire BEFORE writing the
	// IN_PROGRESS record so a "busy" condition leaves no observable side
	// effect — the caller simply retries the same workflow.Run when capacity
	// is available. Released via defer below so every exit path (success,
	// failure, pending) frees the slot for other workflows.
	concurrencyKey := runOptions.GetConcurrencyKey()
	concurrencyLimit := runOptions.GetConcurrencyLimit()
	var acquiredSlotID string
	if concurrencyKey != "" && concurrencyLimit > 0 {
		slotID, err := acquireConcurrencySlot(
			ctx, claimStore,
			storage.DefaultNamespace, concurrencyKey,
			concurrencyLimit, runOptions.GetClaimOwnerId(),
			runOptions.GetCodeVersion(),
			DefaultClaimLeaseDuration,
		)
		if err != nil {
			return zero, err
		}
		if slotID == "" {
			return zero, &ConcurrencyBusyError{Key: concurrencyKey, Limit: concurrencyLimit}
		}
		acquiredSlotID = slotID
		defer func() {
			// Use a fresh context for release so a cancelled parent ctx still
			// frees the slot. Create-only claim expiry does not grant takeover,
			// so a failed release requires verified operator cleanup.
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer releaseCancel()
			if releaseErr := releaseConcurrencySlot(
				releaseCtx,
				claimStore,
				storage.DefaultNamespace,
				concurrencyKey,
				acquiredSlotID,
			); releaseErr != nil {
				returnResult = zero
				returnErr = errors.Join(
					returnErr,
					fmt.Errorf("%w: concurrency slot: %w", ErrClaimRelease, releaseErr),
				)
			}
		}()
	}

	inputAny, err := anypb.New(input)
	if err != nil {
		return zero, err
	}

	if createdAt == nil {
		createdAt = timestamppb.New(time.Now().UTC())
		inProgress := &temporalessv1.WorkflowRecord{
			SchemaVersion: storage.WorkflowRecordSchemaVersion,
			Key:           key.Proto(),
			WorkflowType:  workflowType,
			CodeVersion:   runOptions.GetCodeVersion(),
			Input:         inputAny,
			Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
			CreatedAt:     createdAt,
		}
		if err := store.PutWorkflow(ctx, inProgress); err != nil {
			return zero, err
		}
	}

	workflowContext := &Workflow{
		store:           store,
		claimStore:      claimStore,
		claimCapability: claimCapability,
		workflowID:      runOptions.GetWorkflowId(),
		runID:           runOptions.GetRunId(),
		codeVersion:     runOptions.GetCodeVersion(),
		claimOwner:      runOptions.GetClaimOwnerId(),
	}
	ctx = context.WithValue(ctx, workflowContextKey{}, workflowContext)
	workflowAnnotations := newAnnotationsBag()
	ctx = context.WithValue(ctx, annotationsKey{}, workflowAnnotations)

	result, runErr := execute(ctx, input)
	if runErr != nil {
		if errors.Is(runErr, ErrTimerPending) ||
			errors.Is(runErr, ErrClaimBusy) ||
			errors.Is(runErr, ErrClaimRelease) ||
			errors.Is(runErr, ErrEventPending) ||
			errors.Is(runErr, ErrWorkflowDependencyPending) {
			return zero, runErr
		}
		failure := failureFromError(runErr)
		failed := &temporalessv1.WorkflowRecord{
			SchemaVersion: storage.WorkflowRecordSchemaVersion,
			Key:           key.Proto(),
			WorkflowType:  workflowType,
			CodeVersion:   runOptions.GetCodeVersion(),
			Input:         inputAny,
			Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED,
			Failure:       failure,
			CreatedAt:     createdAt,
			CompletedAt:   timestamppb.New(time.Now().UTC()),
			Annotations:   workflowAnnotations.snapshot(),
		}
		if err := store.PutWorkflow(ctx, failed); err != nil {
			return zero, err
		}
		return zero, runErr
	}
	if isNilMessage(result) {
		return zero, fmt.Errorf("workflow %q returned a nil result", runOptions.GetWorkflowId())
	}

	resultAny, err := anypb.New(result)
	if err != nil {
		return zero, err
	}
	completed := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  workflowType,
		CodeVersion:   runOptions.GetCodeVersion(),
		Input:         inputAny,
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		Result:        resultAny,
		CreatedAt:     createdAt,
		CompletedAt:   timestamppb.New(time.Now().UTC()),
		Annotations:   workflowAnnotations.snapshot(),
	}
	if err := store.PutWorkflow(ctx, completed); err != nil {
		return zero, err
	}
	return result, nil
}

func Current(ctx context.Context) (*Workflow, bool) {
	workflow, ok := ctx.Value(workflowContextKey{}).(*Workflow)
	return workflow, ok && workflow != nil
}

func ExecuteActivity[Req proto.Message, Resp proto.Message](
	ctx context.Context,
	options *ActivityOptions,
	input Req,
	newResult func() Resp,
	execute ActivityFunc[Req, Resp],
) (Resp, error) {
	var zero Resp
	workflow, ok := Current(ctx)
	if !ok {
		return zero, fmt.Errorf("workflow context is required")
	}
	if options == nil {
		return zero, fmt.Errorf("activity options are required")
	}
	if err := protovalidate.Validate(options); err != nil {
		return zero, err
	}
	if isNilMessage(input) {
		return zero, fmt.Errorf("activity input is required")
	}
	if newResult == nil {
		return zero, fmt.Errorf("activity result constructor is required")
	}
	if execute == nil {
		return zero, fmt.Errorf("activity executor is required")
	}

	resultTemplate := newResult()
	if isNilMessage(resultTemplate) {
		return zero, fmt.Errorf("activity result constructor returned nil")
	}
	activityType := messagePairType("activity", input, resultTemplate)

	return runActivity(
		ctx,
		workflow,
		options.GetActivityId(),
		activityType,
		options.GetRetryPolicy(),
		input,
		newResult,
		func(ctx context.Context) (Resp, error) {
			return execute(ctx, input)
		},
	)
}

func WaitEvent[T proto.Message](
	ctx context.Context,
	eventID string,
	newPayload func() T,
) (T, error) {
	var zero T
	workflow, ok := Current(ctx)
	if !ok {
		return zero, fmt.Errorf("workflow context is required")
	}
	if newPayload == nil {
		return zero, fmt.Errorf("event payload constructor is required")
	}
	key := storage.EventKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		EventID:    eventID,
	}
	if err := key.Validate(); err != nil {
		return zero, err
	}
	record, found, err := workflow.store.GetEvent(ctx, key)
	if err != nil {
		return zero, err
	}
	if !found {
		return zero, &EventPendingError{EventID: eventID}
	}
	payload := newPayload()
	if isNilMessage(payload) {
		return zero, fmt.Errorf("event payload constructor returned nil")
	}
	if record.GetPayload() == nil {
		return zero, fmt.Errorf("stored event has no payload")
	}
	if err := record.GetPayload().UnmarshalTo(payload); err != nil {
		return zero, err
	}
	return payload, nil
}

func Sleep(ctx context.Context, timerID string, duration time.Duration) error {
	workflow, ok := Current(ctx)
	if !ok {
		return fmt.Errorf("workflow context is required")
	}

	if strings.HasPrefix(timerID, ActivityRetryTimerIDPrefix) {
		return fmt.Errorf(
			"timer_id %q uses the framework-reserved %q prefix; choose another",
			timerID, ActivityRetryTimerIDPrefix,
		)
	}
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    timerID,
	}
	if err := key.Validate(); err != nil {
		return err
	}
	timerKind := storage.SleepTimerKind

	now := time.Now().UTC()
	record, found, err := workflow.store.GetTimer(ctx, key)
	if err != nil {
		return err
	}
	if found {
		if record.GetTimerKind() != timerKind {
			return fmt.Errorf("%w: timer kind changed from %s to %s", ErrTimerConflict, record.GetTimerKind(), timerKind)
		}
		if record.GetCodeVersion() != workflow.codeVersion {
			return fmt.Errorf("%w: code version changed from %q to %q", ErrTimerConflict, record.GetCodeVersion(), workflow.codeVersion)
		}
		if record.GetDuration().AsDuration() != duration {
			return fmt.Errorf("%w: timer duration changed from %s to %s", ErrTimerConflict, record.GetDuration().AsDuration(), duration)
		}
		if record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
			return nil
		}
		if record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
			return fmt.Errorf("%w: timer was canceled", ErrTimerConflict)
		}
		fireAt := record.GetFireAt().AsTime()
		if now.Before(fireAt) {
			return &TimerPendingError{TimerID: timerID, WakeAt: fireAt}
		}
		record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
		record.FiredAt = timestamppb.New(now)
		return workflow.store.PutTimer(ctx, record)
	}

	fireAt := now.Add(duration)
	status := temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
	var firedAt *timestamppb.Timestamp
	if !now.Before(fireAt) {
		status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
		firedAt = timestamppb.New(now)
	}
	record = &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           key.Proto(),
		TimerKind:     timerKind,
		CodeVersion:   workflow.codeVersion,
		Duration:      durationpb.New(duration),
		Status:        status,
		FireAt:        timestamppb.New(fireAt),
		CreatedAt:     timestamppb.New(now),
		FiredAt:       firedAt,
	}
	if err := workflow.store.PutTimer(ctx, record); err != nil {
		return err
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		return nil
	}
	return &TimerPendingError{TimerID: timerID, WakeAt: fireAt}
}

func getActivityAuthoritative(
	ctx context.Context,
	store storage.Store,
	key storage.ActivityKey,
) (*temporalessv1.ActivityRecord, bool, error) {
	if cache, ok := store.(*runScopedCache); ok {
		return cache.refreshActivity(ctx, key)
	}
	return store.GetActivity(ctx, key)
}

func runActivity[T proto.Message](
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	activityType string,
	retryPolicy *temporalessv1.RetryPolicy,
	input proto.Message,
	newResult func() T,
	execute func(context.Context) (T, error),
) (T, error) {
	var zero T
	if workflow == nil {
		return zero, fmt.Errorf("workflow is required")
	}
	if err := protovalidate.Validate(&temporalessv1.ActivityOptions{ActivityId: activityID}); err != nil {
		return zero, err
	}
	if activityType == "" {
		return zero, fmt.Errorf("activity type is required")
	}
	if isNilMessage(input) {
		return zero, fmt.Errorf("activity input is required")
	}
	if newResult == nil {
		return zero, fmt.Errorf("activity result constructor is required")
	}
	if execute == nil {
		return zero, fmt.Errorf("activity executor is required")
	}

	plan, err := planRetries(retryPolicy)
	if err != nil {
		return zero, err
	}

	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		ActivityID: activityID,
	}

	record, found, err := workflow.store.GetActivity(ctx, key)
	if err != nil {
		return zero, err
	}

	var attempts []*temporalessv1.ActivityAttempt
	retryTimerDue := false
	if found {
		switch record.GetStatus() {
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
			temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
			return replayRecord(record, activityType, workflow.codeVersion, newResult)
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
			if err := assertActivityIdentity(record, activityType, workflow.codeVersion); err != nil {
				return zero, err
			}
			// Durable retry resume: if the record carries next_attempt_at and
			// the wake instant hasn't arrived yet, bail back to the workflow
			// as pending. The paired TIMER_KIND_ACTIVITY_RETRY timer keeps
			// the scanner waking the workflow until fire_at.
			if nextAt := record.GetNextAttemptAt(); nextAt != nil {
				wakeAt := nextAt.AsTime()
				if time.Now().UTC().Before(wakeAt) {
					return zero, &TimerPendingError{
						TimerID: activityRetryTimerID(activityID),
						WakeAt:  wakeAt,
					}
				}
				retryTimerDue = true
			}
			attempts = record.GetAttempts()
		default:
			return zero, fmt.Errorf("%w: stored activity has unknown status", ErrActivityConflict)
		}
	}
	inputAny, err := anypb.New(input)
	if err != nil {
		return zero, err
	}

	var activityClaimKey storage.ClaimKey
	claimAcquired := false
	releaseActivityClaim := func(result T, outcome error) (T, error) {
		if !claimAcquired {
			return result, outcome
		}
		// Preserve request-scoped auth/routing values for a remote claim store,
		// while detaching cleanup from caller cancellation and its old deadline.
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer releaseCancel()
		if _, releaseErr := workflow.claimStore.DeleteClaim(releaseCtx, activityClaimKey); releaseErr != nil {
			return zero, errors.Join(
				outcome,
				fmt.Errorf("%w: activity claim %q: %w", ErrClaimRelease, activityID, releaseErr),
			)
		}
		claimAcquired = false
		return result, outcome
	}

	// Activity-level claims are opt-in via claim_owner_id. Existing claims are
	// always busy, including ones with the same owner: concurrent fan-out calls
	// within one workflow share an owner and must not execute the same activity
	// twice. A crashed create-only claim requires verified operator cleanup.
	if workflow.claimStore != nil && workflow.claimOwner != "" {
		activityClaimKey = storage.ClaimKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: workflow.workflowID,
			RunID:      workflow.runID,
			ClaimID:    ActivityClaimIDPrefix + activityID,
		}
		for claimAttempt := 0; claimAttempt < 2; claimAttempt++ {
			now := time.Now().UTC()
			created, createErr := workflow.claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
				SchemaVersion:  storage.ClaimRecordSchemaVersion,
				Key:            activityClaimKey.Proto(),
				OwnerId:        workflow.claimOwner,
				ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
				ResourceId:     activityID,
				CodeVersion:    workflow.codeVersion,
				LeaseExpiresAt: timestamppb.New(now.Add(DefaultClaimLeaseDuration)),
				CreatedAt:      timestamppb.New(now),
				HeartbeatAt:    timestamppb.New(now),
			})
			if createErr != nil {
				return zero, createErr
			}
			if created {
				claimAcquired = true
				break
			}

			// Bypass a cached miss: the winner may have committed a terminal
			// record between our initial read and failed conditional create.
			fresh, freshFound, freshErr := getActivityAuthoritative(ctx, workflow.store, key)
			if freshErr != nil {
				return zero, freshErr
			}
			if freshFound && fresh.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
				return replayRecord(fresh, activityType, workflow.codeVersion, newResult)
			}

			existing, claimFound, getErr := workflow.claimStore.GetClaim(ctx, activityClaimKey)
			if getErr != nil {
				return zero, getErr
			}
			if claimFound || claimAttempt == 1 {
				busy := &ClaimBusyError{
					ClaimID:    activityClaimKey.ClaimID,
					Capability: workflow.claimCapability,
				}
				if claimFound {
					busy.OwnerID = existing.GetOwnerId()
					if existing.GetLeaseExpiresAt() != nil {
						busy.LeaseExpiresAt = existing.GetLeaseExpiresAt().AsTime()
					}
				}
				return zero, busy
			}
		}
		if !claimAcquired {
			return zero, fmt.Errorf("failed to acquire activity claim %q", activityID)
		}

		// A prior holder may have committed and released after our initial read.
		// Refresh through the cache's underlying store before executing anything.
		fresh, freshFound, freshErr := getActivityAuthoritative(ctx, workflow.store, key)
		if freshErr != nil {
			// Storage outcome is ambiguous; retain the claim for operator recovery.
			return zero, freshErr
		}
		record, found = fresh, freshFound
		attempts = nil
		retryTimerDue = false
		if found {
			switch record.GetStatus() {
			case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
				temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
				replayed, replayErr := replayRecord(record, activityType, workflow.codeVersion, newResult)
				return releaseActivityClaim(replayed, replayErr)
			case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
				if identityErr := assertActivityIdentity(record, activityType, workflow.codeVersion); identityErr != nil {
					return releaseActivityClaim(zero, identityErr)
				}
				if nextAt := record.GetNextAttemptAt(); nextAt != nil {
					wakeAt := nextAt.AsTime()
					if time.Now().UTC().Before(wakeAt) {
						return releaseActivityClaim(zero, &TimerPendingError{
							TimerID: activityRetryTimerID(activityID),
							WakeAt:  wakeAt,
						})
					}
					retryTimerDue = true
				}
				attempts = record.GetAttempts()
			default:
				return releaseActivityClaim(
					zero,
					fmt.Errorf("%w: stored activity has unknown status", ErrActivityConflict),
				)
			}
		}
	}

	if retryTimerDue {
		// Mark the paired timer only after claim arbitration. A storage error here
		// leaves execution ownership ambiguous, so retain the activity claim.
		if err := markActivityRetryTimerFired(ctx, workflow, activityID); err != nil {
			return zero, err
		}
	}

	if attempts == nil {
		attempts = make([]*temporalessv1.ActivityAttempt, 0, plan.maxAttempts)
	}
	interval := plan.initialInterval
	activityAnnotations := newAnnotationsBag()
	if found && record != nil {
		// Restore annotations from the prior RETRYING record so per-attempt
		// metadata (model, tokens, vendor, etc.) survives cross-invocation
		// resumes.
		for k, v := range record.GetAnnotations() {
			activityAnnotations.set(k, v)
		}
	}
	activityCtx := context.WithValue(ctx, annotationsKey{}, activityAnnotations)
	startIdx := uint32(len(attempts)) + 1

	for attemptIdx := startIdx; attemptIdx <= plan.maxAttempts; attemptIdx++ {
		startedAt := time.Now().UTC()
		result, runErr := execute(activityCtx)
		completedAt := time.Now().UTC()

		if runErr == nil {
			if isNilMessage(result) {
				return zero, fmt.Errorf("activity %q returned a nil result", activityID)
			}
			attempts = append(attempts, &temporalessv1.ActivityAttempt{
				Attempt:     attemptIdx,
				StartedAt:   timestamppb.New(startedAt),
				CompletedAt: timestamppb.New(completedAt),
			})
			resultAny, err := anypb.New(result)
			if err != nil {
				return zero, err
			}
			completedRecord := &temporalessv1.ActivityRecord{
				SchemaVersion: storage.ActivityRecordSchemaVersion,
				Key:           key.Proto(),
				ActivityType:  activityType,
				CodeVersion:   workflow.codeVersion,
				Input:         inputAny,
				Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
				Result:        resultAny,
				CreatedAt:     timestamppb.New(attempts[0].GetStartedAt().AsTime()),
				CompletedAt:   timestamppb.New(completedAt),
				Attempts:      attempts,
				Annotations:   activityAnnotations.snapshot(),
			}
			if err := workflow.store.PutActivity(ctx, completedRecord); err != nil {
				// The terminal write may have committed despite the returned error.
				// Retain the claim rather than permit an unsafe automatic rerun.
				return zero, err
			}
			return releaseActivityClaim(result, nil)
		}

		failure := failureFromError(runErr)
		attempts = append(attempts, &temporalessv1.ActivityAttempt{
			Attempt:     attemptIdx,
			StartedAt:   timestamppb.New(startedAt),
			CompletedAt: timestamppb.New(completedAt),
			Failure:     failure,
		})

		// Vendor-supplied Retry-After overrides the computed interval when
		// it's longer. The retry policy's exponential schedule still applies
		// as a floor — so an aggressive policy doesn't undershoot a vendor's
		// stated rate-limit window.
		if ra := failure.GetRetryAfter().AsDuration(); ra > interval {
			interval = ra
		}

		nonRetryable := plan.nonRetryable[failure.GetCode()]
		if attemptIdx >= plan.maxAttempts || nonRetryable {
			failedRecord := &temporalessv1.ActivityRecord{
				SchemaVersion: storage.ActivityRecordSchemaVersion,
				Key:           key.Proto(),
				ActivityType:  activityType,
				CodeVersion:   workflow.codeVersion,
				Input:         inputAny,
				Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED,
				Failure:       failure,
				CreatedAt:     timestamppb.New(attempts[0].GetStartedAt().AsTime()),
				CompletedAt:   timestamppb.New(completedAt),
				Attempts:      attempts,
				Annotations:   activityAnnotations.snapshot(),
			}
			if err := workflow.store.PutActivity(ctx, failedRecord); err != nil {
				// Retain on an ambiguous terminal write.
				return zero, err
			}
			return releaseActivityClaim(
				zero,
				&ActivityError{Code: failure.GetCode(), Message: failure.GetMessage(), Cause: runErr},
			)
		}

		retryingRecord := &temporalessv1.ActivityRecord{
			SchemaVersion: storage.ActivityRecordSchemaVersion,
			Key:           key.Proto(),
			ActivityType:  activityType,
			CodeVersion:   workflow.codeVersion,
			Input:         inputAny,
			Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
			Failure:       failure,
			CreatedAt:     timestamppb.New(attempts[0].GetStartedAt().AsTime()),
			Attempts:      attempts,
			Annotations:   activityAnnotations.snapshot(),
		}

		// Durable retry branch: when the next backoff interval crosses the
		// configured threshold, persist the wait as a TIMER_KIND_ACTIVITY_RETRY
		// timer and surface a typed pending error. The timer scanner re-invokes
		// the workflow after fire_at; runActivity then enters the RETRYING-resume
		// branch above and continues the loop.
		if plan.durableThreshold > 0 && interval >= plan.durableThreshold {
			nextAttemptAt := time.Now().UTC().Add(interval)
			retryingRecord.NextAttemptAt = timestamppb.New(nextAttemptAt)
			if err := workflow.store.PutActivity(ctx, retryingRecord); err != nil {
				// Retain on an ambiguous retry-state write.
				return zero, err
			}
			if err := putActivityRetryTimer(ctx, workflow, activityID, interval, nextAttemptAt); err != nil {
				// The retry record is durable but the wakeup write failed. Retain
				// ownership so an operator can reconcile the partial boundary.
				return zero, err
			}
			return releaseActivityClaim(zero, &TimerPendingError{
				TimerID: activityRetryTimerID(activityID),
				WakeAt:  nextAttemptAt,
			})
		}

		if err := workflow.store.PutActivity(ctx, retryingRecord); err != nil {
			// Retain on an ambiguous retry-state write.
			return zero, err
		}

		if err := sleepCtx(ctx, interval); err != nil {
			// The failed attempt and RETRYING record are durable. Releasing is
			// safe even when the caller canceled during the in-process backoff.
			return releaseActivityClaim(zero, err)
		}
		interval = nextInterval(interval, plan)
	}

	return zero, fmt.Errorf("activity %q exhausted retry plan", activityID)
}

// assertActivityIdentity guards against shape changes that would make the
// stored record incompatible with the current code path: a swapped
// request/response message type (which changes activity_type) or a bumped
// code_version. The activity_id itself is the de-duplication key; same id +
// same shape + same code_version is treated as the same logical activity
// regardless of the input bytes — the caller chose the id and owns its
// semantics.
func assertActivityIdentity(
	record *temporalessv1.ActivityRecord,
	activityType string,
	codeVersion string,
) error {
	if record.GetActivityType() != activityType {
		return fmt.Errorf("%w: activity type changed from %q to %q", ErrActivityConflict, record.GetActivityType(), activityType)
	}
	if record.GetCodeVersion() != codeVersion {
		return fmt.Errorf("%w: code version changed from %q to %q", ErrActivityConflict, record.GetCodeVersion(), codeVersion)
	}
	return nil
}

func replayRecord[T proto.Message](
	record *temporalessv1.ActivityRecord,
	activityType string,
	codeVersion string,
	newResult func() T,
) (T, error) {
	var zero T
	if err := assertActivityIdentity(record, activityType, codeVersion); err != nil {
		return zero, err
	}

	switch record.GetStatus() {
	case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED:
		if record.GetResult() == nil {
			return zero, fmt.Errorf("%w: stored activity has no result", ErrActivityConflict)
		}
		result := newResult()
		if isNilMessage(result) {
			return zero, fmt.Errorf("activity result constructor returned nil")
		}
		if err := record.GetResult().UnmarshalTo(result); err != nil {
			return zero, err
		}
		return result, nil
	case temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
		failure := record.GetFailure()
		return zero, &ActivityError{Code: failure.GetCode(), Message: failure.GetMessage()}
	default:
		return zero, fmt.Errorf("%w: stored activity has unknown status", ErrActivityConflict)
	}
}

type retryPlan struct {
	maxAttempts        uint32
	initialInterval    time.Duration
	backoffCoefficient float64
	maximumInterval    time.Duration
	durableThreshold   time.Duration
	nonRetryable       map[string]bool
}

func planRetries(policy *temporalessv1.RetryPolicy) (retryPlan, error) {
	if policy == nil {
		return retryPlan{maxAttempts: 1}, nil
	}
	plan := retryPlan{
		maxAttempts:        policy.GetMaximumAttempts(),
		initialInterval:    policy.GetInitialInterval().AsDuration(),
		backoffCoefficient: policy.GetBackoffCoefficient(),
		maximumInterval:    policy.GetMaximumInterval().AsDuration(),
		durableThreshold:   policy.GetDurableBackoffThreshold().AsDuration(),
	}
	if plan.maxAttempts == 0 {
		return retryPlan{}, fmt.Errorf("retry policy maximum_attempts must be > 0")
	}
	if plan.maxAttempts > 1 && plan.initialInterval <= 0 {
		return retryPlan{}, fmt.Errorf("retry policy initial_interval must be > 0 when maximum_attempts > 1")
	}
	if plan.backoffCoefficient == 0 {
		plan.backoffCoefficient = 1.0
	}
	if plan.durableThreshold < 0 {
		return retryPlan{}, fmt.Errorf("retry policy durable_backoff_threshold must be >= 0")
	}
	if codes := policy.GetNonRetryableErrorCodes(); len(codes) > 0 {
		plan.nonRetryable = make(map[string]bool, len(codes))
		for _, code := range codes {
			plan.nonRetryable[code] = true
		}
	}
	return plan, nil
}

// putActivityRetryTimer writes (or overwrites) the TIMER_KIND_ACTIVITY_RETRY
// timer paired with an activity's durable retry. Stable per activity_id so
// later retries naturally overwrite earlier scheduled state.
func putActivityRetryTimer(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	duration time.Duration,
	fireAt time.Time,
) error {
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    activityRetryTimerID(activityID),
	}
	record := &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           key.Proto(),
		TimerKind:     temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
		CodeVersion:   workflow.codeVersion,
		Duration:      durationpb.New(duration),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(fireAt),
		CreatedAt:     timestamppb.New(time.Now().UTC()),
	}
	return workflow.store.PutTimer(ctx, record)
}

// markActivityRetryTimerFired transitions the paired retry timer to FIRED so
// the timer scanner stops returning it while the activity body is executing
// the resumed attempt. No-op if the timer record is absent (legacy path).
func markActivityRetryTimerFired(ctx context.Context, workflow *Workflow, activityID string) error {
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    activityRetryTimerID(activityID),
	}
	record, found, err := workflow.store.GetTimer(ctx, key)
	if err != nil {
		return err
	}
	if !found || record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		return nil
	}
	record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
	record.FiredAt = timestamppb.New(time.Now().UTC())
	return workflow.store.PutTimer(ctx, record)
}

func nextInterval(prev time.Duration, plan retryPlan) time.Duration {
	next := time.Duration(float64(prev) * plan.backoffCoefficient)
	if plan.maximumInterval > 0 && next > plan.maximumInterval {
		next = plan.maximumInterval
	}
	return next
}

func sleepCtx(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func failureFromError(err error) *temporalessv1.ActivityFailure {
	var typed *ActivityError
	if errors.As(err, &typed) {
		failure := &temporalessv1.ActivityFailure{Code: typed.Code, Message: typed.Message}
		if typed.RetryAfter > 0 {
			failure.RetryAfter = durationpb.New(typed.RetryAfter)
		}
		return failure
	}
	return &temporalessv1.ActivityFailure{Message: err.Error()}
}

func replayWorkflowRecord[T proto.Message](
	record *temporalessv1.WorkflowRecord,
	workflowType string,
	codeVersion string,
	newResult func() T,
) (T, error) {
	var zero T
	if err := assertWorkflowIdentity(record, workflowType, codeVersion); err != nil {
		return zero, err
	}
	switch record.GetStatus() {
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
		if record.GetResult() == nil {
			return zero, fmt.Errorf("%w: stored workflow has no result", ErrWorkflowConflict)
		}
		result := newResult()
		if isNilMessage(result) {
			return zero, fmt.Errorf("workflow result constructor returned nil")
		}
		if err := record.GetResult().UnmarshalTo(result); err != nil {
			return zero, err
		}
		return result, nil
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		failure := record.GetFailure()
		return zero, &ActivityError{Code: failure.GetCode(), Message: failure.GetMessage()}
	default:
		return zero, fmt.Errorf("%w: stored workflow has unknown status", ErrWorkflowConflict)
	}
}

// assertWorkflowIdentity guards against shape changes. See
// assertActivityIdentity for the de-duplication contract.
func assertWorkflowIdentity(
	record *temporalessv1.WorkflowRecord,
	workflowType string,
	codeVersion string,
) error {
	if record.GetWorkflowType() != workflowType {
		return fmt.Errorf("%w: workflow type changed from %q to %q", ErrWorkflowConflict, record.GetWorkflowType(), workflowType)
	}
	if record.GetCodeVersion() != codeVersion {
		return fmt.Errorf("%w: code version changed from %q to %q", ErrWorkflowConflict, record.GetCodeVersion(), codeVersion)
	}
	return nil
}

func messagePairType(kind string, input proto.Message, output proto.Message) string {
	return fmt.Sprintf(
		"%s:%s->%s",
		kind,
		input.ProtoReflect().Descriptor().FullName(),
		output.ProtoReflect().Descriptor().FullName(),
	)
}

func codeVersionFromEnv() string {
	if value := os.Getenv("TEMPORALESS_CODE_VERSION"); value != "" {
		return value
	}
	return "dev"
}

func isNilMessage(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func normalizedWorkflowOptions(options *Options) (*Options, error) {
	if options == nil {
		return nil, fmt.Errorf("workflow options are required")
	}
	normalized := proto.Clone(options).(*temporalessv1.WorkflowOptions)
	if normalized.GetCodeVersion() == "" {
		normalized.CodeVersion = codeVersionFromEnv()
	}
	if err := protovalidate.Validate(normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}
