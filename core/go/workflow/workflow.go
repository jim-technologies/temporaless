package workflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"sort"
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
var ErrWorkflowInfrastructure = errors.New("workflow infrastructure operation failed")

const DefaultClaimLeaseDuration = time.Duration(temporalessv1.Default_RuntimeDefaults_ClaimLeaseDurationSeconds) * time.Second

// ActivityClaimIDPrefix namespaces claims that serialize one activity ID.
// Sourced from ReservedNames so Go and Python persist identical claim keys.
var ActivityClaimIDPrefix = temporalessv1.Default_ReservedNames_ActivityClaimIdPrefix

// WorkflowExecutionClaimID is the deterministic claim_id used to serialize
// live invocations of one workflow run. The workflow_id and run_id live in the
// surrounding ClaimKey. Sourced from ReservedNames so SDKs cannot drift.
var WorkflowExecutionClaimID = temporalessv1.Default_ReservedNames_WorkflowExecutionClaimId

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

// WorkflowInfrastructureError marks a framework storage or coordination
// operation that failed while a workflow body was using a Temporaless
// primitive. The operation is safe to re-invoke: Run leaves the parent
// WorkflowRecord IN_PROGRESS instead of turning a transient backend outage
// into an application failure.
//
// Activity bodies are still application boundaries. If an activity itself
// returns this error, runActivity records it as an ordinary activity failure;
// callers cannot accidentally bypass activity retry/error semantics by
// returning a framework error value from business code.
type WorkflowInfrastructureError struct {
	Operation string
	Cause     error
}

func (err *WorkflowInfrastructureError) Error() string {
	if err.Operation == "" {
		return fmt.Sprintf("workflow infrastructure operation failed: %v", err.Cause)
	}
	return fmt.Sprintf("workflow infrastructure operation %q failed: %v", err.Operation, err.Cause)
}

func (err *WorkflowInfrastructureError) Unwrap() error {
	return err.Cause
}

func (err *WorkflowInfrastructureError) Is(target error) bool {
	return target == ErrWorkflowInfrastructure
}

func workflowInfrastructureError(operation string, cause error) error {
	if cause == nil {
		return nil
	}
	// Corrupt records are durable invariant violations, not transient backend
	// failures. Let the normal terminal/conflict path expose them rather than
	// retrying forever.
	if errors.Is(cause, storage.ErrCorruptRecord) {
		return cause
	}
	var infrastructureErr *WorkflowInfrastructureError
	if errors.As(cause, &infrastructureErr) {
		return cause
	}
	return &WorkflowInfrastructureError{Operation: operation, Cause: cause}
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

	consumedSleepMu     sync.Mutex
	consumedSleepTimers map[string]storage.TimerKey
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

// registerConsumedSleepTimer records that this invocation crossed a due
// durable-sleep boundary. The authoritative TimerRecord intentionally remains
// SCHEDULED until the invocation commits either a later wake-bearing timer or
// the terminal WorkflowRecord. That ordering prevents a process crash between
// Sleep returning and the next durable boundary from losing the only wakeup.
func (w *Workflow) registerConsumedSleepTimer(key storage.TimerKey) {
	w.consumedSleepMu.Lock()
	defer w.consumedSleepMu.Unlock()
	if w.consumedSleepTimers == nil {
		w.consumedSleepTimers = make(map[string]storage.TimerKey)
	}
	w.consumedSleepTimers[key.TimerID] = key
}

// acknowledgeConsumedSleepTimers marks previously consumed sleep timers FIRED
// after a durable successor has committed. exceptTimerID protects a timer that
// is itself the new successor (including repeated calls using the same ID).
// Successful updates are forgotten; failed ones remain registered so another
// boundary in the same invocation can retry. Callers keep cleanup best-effort:
// the successor is already durable and any still-SCHEDULED timer is a safe
// duplicate wake rather than a lost wake.
func (w *Workflow) acknowledgeConsumedSleepTimers(ctx context.Context, exceptTimerID string) error {
	w.consumedSleepMu.Lock()
	timerIDs := make([]string, 0, len(w.consumedSleepTimers))
	keys := make(map[string]storage.TimerKey, len(w.consumedSleepTimers))
	for timerID, key := range w.consumedSleepTimers {
		if timerID == exceptTimerID {
			continue
		}
		timerIDs = append(timerIDs, timerID)
		keys[timerID] = key
	}
	w.consumedSleepMu.Unlock()
	if len(timerIDs) == 0 {
		return nil
	}
	sort.Strings(timerIDs)

	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cleanupCancel()
	var cleanupErrors []error
	for _, timerID := range timerIDs {
		key := keys[timerID]
		record, found, err := w.store.GetTimer(cleanupCtx, key)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("get consumed sleep timer %q: %w", timerID, err))
			continue
		}
		if !found || record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED ||
			record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
			w.forgetConsumedSleepTimer(timerID)
			continue
		}
		if record.GetTimerKind() != storage.SleepTimerKind {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("%w: consumed timer %q is not a sleep timer", ErrTimerConflict, timerID))
			continue
		}
		if record.GetCodeVersion() != w.codeVersion {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("%w: consumed timer %q code version changed", ErrTimerConflict, timerID))
			continue
		}
		updated := proto.Clone(record).(*temporalessv1.TimerRecord)
		updated.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
		updated.FiredAt = timestamppb.New(time.Now().UTC())
		if err := w.store.PutTimer(cleanupCtx, updated); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("acknowledge consumed sleep timer %q: %w", timerID, err))
			continue
		}
		w.forgetConsumedSleepTimer(timerID)
	}
	return errors.Join(cleanupErrors...)
}

func (w *Workflow) forgetConsumedSleepTimer(timerID string) {
	w.consumedSleepMu.Lock()
	defer w.consumedSleepMu.Unlock()
	delete(w.consumedSleepTimers, timerID)
}

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
			result, replayErr := replayWorkflowRecord(
				record,
				workflowType,
				runOptions.GetCodeVersion(),
				runOptions.GetRunOrderTime(),
				newResult,
			)
			return result, nil, true, replayErr
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS:
			if identityErr := assertWorkflowIdentity(
				record,
				workflowType,
				runOptions.GetCodeVersion(),
				runOptions.GetRunOrderTime(),
			); identityErr != nil {
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
			RunOrderTime:  runOptions.GetRunOrderTime(),
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
		if isWorkflowContinuationError(runErr) {
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
			RunOrderTime:  runOptions.GetRunOrderTime(),
		}
		if err := store.PutWorkflow(ctx, failed); err != nil {
			// No terminal boundary committed: leave any consumed due sleeps
			// SCHEDULED so the scanner can redeliver this workflow.
			return zero, err
		}
		// FAILED is now authoritative. Timer acknowledgement is best-effort and
		// cannot replace the workflow body's terminal error.
		_ = workflowContext.acknowledgeConsumedSleepTimers(ctx, "")
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
		RunOrderTime:  runOptions.GetRunOrderTime(),
	}
	if err := store.PutWorkflow(ctx, completed); err != nil {
		// No terminal boundary committed: retain consumed due sleeps as wakes.
		return zero, err
	}
	// COMPLETED is authoritative; stale ledger entries are also safely pruned
	// by DueTimers if this best-effort acknowledgement cannot be written.
	_ = workflowContext.acknowledgeConsumedSleepTimers(ctx, "")
	return result, nil
}

func isWorkflowContinuationError(err error) bool {
	// Claim cleanup failure remains non-terminal even when it is joined with an
	// activity outcome: a leaked create-only claim can otherwise make the
	// supposedly terminal transition operationally unrecoverable.
	if errors.Is(err, ErrClaimRelease) {
		return true
	}
	// An activity body is a business boundary. It may itself return one of the
	// framework's typed errors; runActivity wraps that value in ActivityError
	// after persisting the activity outcome. Do not misclassify the nested cause
	// as a workflow continuation signal.
	var activityErr *ActivityError
	if errors.As(err, &activityErr) {
		return false
	}
	if errors.Is(err, ErrWorkflowConflict) ||
		errors.Is(err, ErrActivityConflict) ||
		errors.Is(err, ErrTimerConflict) ||
		errors.Is(err, storage.ErrCorruptRecord) {
		return false
	}
	return errors.Is(err, ErrTimerPending) ||
		errors.Is(err, ErrClaimBusy) ||
		errors.Is(err, ErrEventPending) ||
		errors.Is(err, ErrWorkflowDependencyPending) ||
		errors.Is(err, ErrWorkflowInfrastructure)
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
		options.GetRetryTimerId(),
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
		return zero, workflowInfrastructureError("read workflow event", err)
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
	if duration < 0 {
		return fmt.Errorf("sleep duration must not be negative")
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
		return workflowInfrastructureError("read sleep timer", err)
	}
	if found {
		fireAt, status, validationErr := sleepTimerDetails(record, key, workflow.codeVersion, duration)
		if validationErr != nil {
			return validationErr
		}
		if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
			return nil
		}
		if status == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
			return fmt.Errorf("%w: timer was canceled", ErrTimerConflict)
		}
		// This SCHEDULED timer is itself a durable successor wake for any
		// earlier sleep consumed by the same invocation. A repeated call with
		// the same timer ID cannot acknowledge itself.
		_ = workflow.acknowledgeConsumedSleepTimers(ctx, timerID)
		if now.Before(fireAt) {
			return &TimerPendingError{TimerID: timerID, WakeAt: fireAt}
		}
		// Do not mark FIRED here. Sleep returning is not durable: a process can
		// still crash before the workflow commits a later wake or terminal
		// record. Keeping this timer SCHEDULED guarantees scanner redelivery.
		workflow.registerConsumedSleepTimer(key)
		return nil
	}

	fireAt := now.Add(duration)
	record = &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           key.Proto(),
		TimerKind:     timerKind,
		CodeVersion:   workflow.codeVersion,
		Duration:      durationpb.New(duration),
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.New(fireAt),
		CreatedAt:     timestamppb.New(now),
	}
	if writeErr := workflow.store.PutTimer(ctx, record); writeErr != nil {
		// A remote store may commit the timer and still lose the response. Read
		// the authoritative point record with a fresh context so the cache and a
		// canceled request cannot hide a committed wakeup.
		verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		verified, verifiedFound, readErr := getTimerAuthoritative(verifyCtx, workflow.store, key)
		infrastructureErr := workflowInfrastructureError("persist sleep timer", writeErr)
		if readErr != nil {
			return workflowInfrastructureError(
				"verify ambiguous sleep timer write",
				errors.Join(writeErr, readErr),
			)
		}
		if !verifiedFound {
			// The write definitely did not land. The typed infrastructure error
			// keeps the workflow IN_PROGRESS and tells the caller to re-invoke it.
			return infrastructureErr
		}
		verifiedFireAt, status, validationErr := sleepTimerDetails(
			verified,
			key,
			workflow.codeVersion,
			duration,
		)
		if validationErr != nil {
			return errors.Join(validationErr, infrastructureErr)
		}
		if status == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
			// Even an already-due verified write returns pending once. This stops
			// the ambiguous invocation at a durable boundary; the scanner can
			// immediately redeliver it without relying solely on the requester.
			return errors.Join(
				&TimerPendingError{TimerID: timerID, WakeAt: verifiedFireAt},
				infrastructureErr,
			)
		}
		if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
			return nil
		}
		return errors.Join(
			fmt.Errorf("%w: timer was canceled", ErrTimerConflict),
			infrastructureErr,
		)
	}
	// The new SCHEDULED timer is a durable successor for earlier consumed
	// sleeps. A same-ID timer is excluded so it never acknowledges itself.
	_ = workflow.acknowledgeConsumedSleepTimers(ctx, timerID)
	if now.Before(fireAt) {
		return &TimerPendingError{TimerID: timerID, WakeAt: fireAt}
	}
	workflow.registerConsumedSleepTimer(key)
	return nil
}

func sleepTimerDetails(
	record *temporalessv1.TimerRecord,
	key storage.TimerKey,
	codeVersion string,
	duration time.Duration,
) (time.Time, temporalessv1.TimerStatus, error) {
	if err := storage.ValidateTimerRecord(record, key); err != nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: invalid sleep timer record: %w",
			ErrTimerConflict,
			err,
		)
	}
	if record.GetTimerKind() != storage.SleepTimerKind {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: timer kind changed from %s to %s",
			ErrTimerConflict,
			record.GetTimerKind(),
			storage.SleepTimerKind,
		)
	}
	if record.GetRetryActivityId() != "" {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer is linked to retry activity %q",
			ErrTimerConflict,
			record.GetRetryActivityId(),
		)
	}
	if record.GetCodeVersion() != codeVersion {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: code version changed from %q to %q",
			ErrTimerConflict,
			record.GetCodeVersion(),
			codeVersion,
		)
	}
	if record.GetDuration() == nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has no duration",
			ErrTimerConflict,
		)
	}
	if err := record.GetDuration().CheckValid(); err != nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has invalid duration: %w",
			ErrTimerConflict,
			err,
		)
	}
	storedDuration := record.GetDuration().AsDuration()
	if !proto.Equal(record.GetDuration(), durationpb.New(storedDuration)) {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer duration is outside Go time.Duration range",
			ErrTimerConflict,
		)
	}
	if storedDuration < 0 {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has negative duration",
			ErrTimerConflict,
		)
	}
	if storedDuration != duration {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: timer duration changed from %s to %s",
			ErrTimerConflict,
			storedDuration,
			duration,
		)
	}
	if record.GetCreatedAt() == nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has no created_at",
			ErrTimerConflict,
		)
	}
	if err := record.GetCreatedAt().CheckValid(); err != nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has invalid created_at: %w",
			ErrTimerConflict,
			err,
		)
	}
	if record.GetFireAt() == nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has no fire_at",
			ErrTimerConflict,
		)
	}
	if err := record.GetFireAt().CheckValid(); err != nil {
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: sleep timer has invalid fire_at: %w",
			ErrTimerConflict,
			err,
		)
	}

	status := record.GetStatus()
	switch status {
	case temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED:
		if record.GetFiredAt() != nil {
			return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
				"%w: scheduled sleep timer has fired_at",
				ErrTimerConflict,
			)
		}
	case temporalessv1.TimerStatus_TIMER_STATUS_FIRED:
		if record.GetFiredAt() == nil {
			return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
				"%w: fired sleep timer has no fired_at",
				ErrTimerConflict,
			)
		}
		if err := record.GetFiredAt().CheckValid(); err != nil {
			return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
				"%w: fired sleep timer has invalid fired_at: %w",
				ErrTimerConflict,
				err,
			)
		}
	case temporalessv1.TimerStatus_TIMER_STATUS_CANCELED:
		if record.GetFiredAt() != nil {
			return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
				"%w: canceled sleep timer has fired_at",
				ErrTimerConflict,
			)
		}
	default:
		return time.Time{}, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: timer has unknown status %s",
			ErrTimerConflict,
			status,
		)
	}
	return record.GetFireAt().AsTime(), status, nil
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

func getTimerAuthoritative(
	ctx context.Context,
	store storage.Store,
	key storage.TimerKey,
) (*temporalessv1.TimerRecord, bool, error) {
	if cache, ok := store.(*runScopedCache); ok {
		return cache.refreshTimer(ctx, key)
	}
	return store.GetTimer(ctx, key)
}

func runActivity[T proto.Message](
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	activityType string,
	retryPolicy *temporalessv1.RetryPolicy,
	retryTimerID string,
	input proto.Message,
	newResult func() T,
	execute func(context.Context) (T, error),
) (T, error) {
	var zero T
	if workflow == nil {
		return zero, fmt.Errorf("workflow is required")
	}
	if err := protovalidate.Validate(&temporalessv1.ActivityOptions{
		ActivityId:   activityID,
		RetryPolicy:  retryPolicy,
		RetryTimerId: retryTimerID,
	}); err != nil {
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
	normalizedRetryPolicy := normalizeRetryPolicy(retryPolicy, plan)

	key := storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		ActivityID: activityID,
	}

	record, found, err := workflow.store.GetActivity(ctx, key)
	if err != nil {
		return zero, workflowInfrastructureError("read activity", err)
	}

	var attempts []*temporalessv1.ActivityAttempt
	if found {
		switch record.GetStatus() {
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
			temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
			if activityRecordMayHaveRetryTimer(record) {
				// The terminal ActivityRecord is authoritative. Best-effort timer
				// reconciliation must never replace its result or failure.
				_ = markActivityRetryTimerFired(ctx, workflow, activityID, record.GetRetryTimerId())
			}
			return replayRecord(record, activityType, workflow.codeVersion, newResult)
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
			if err := assertActivityIdentity(record, activityType, workflow.codeVersion); err != nil {
				return zero, err
			}
			if err := assertRetryingActivity(record, normalizedRetryPolicy, retryTimerID, plan); err != nil {
				return zero, err
			}
			// Durable retry resume: if the record carries next_attempt_at and
			// the wake instant hasn't arrived yet, bail back to the workflow
			// as pending. The paired TIMER_KIND_ACTIVITY_RETRY timer keeps
			// the scanner waking the workflow until fire_at.
			if nextAt := record.GetNextAttemptAt(); nextAt != nil {
				wakeAt, ensureErr := ensureActivityRetryTimer(ctx, workflow, activityID, retryTimerID, record, plan)
				if ensureErr != nil {
					return zero, ensureErr
				}
				// Only a verified SCHEDULED retry timer is a durable successor
				// wake for ordinary sleeps consumed earlier in this invocation.
				_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
				if time.Now().UTC().Before(wakeAt) {
					return zero, &TimerPendingError{
						TimerID: retryTimerID,
						WakeAt:  wakeAt,
					}
				}
			} else if plan.durableThreshold > 0 {
				wakeAt, prepared, prepareErr := newerPreparedActivityRetryTimer(
					ctx,
					workflow,
					activityID,
					retryTimerID,
					record,
					plan,
				)
				if prepareErr != nil {
					return zero, prepareErr
				}
				if prepared {
					_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
					if time.Now().UTC().Before(wakeAt) {
						return zero, &TimerPendingError{
							TimerID: retryTimerID,
							WakeAt:  wakeAt,
						}
					}
				}
			}
			attempts = record.GetAttempts()
		default:
			return zero, fmt.Errorf("%w: stored activity has unknown status", ErrActivityConflict)
		}
	}
	if !found && plan.maxAttempts > 1 && plan.durableThreshold > 0 {
		wakeAt, prepared, prepareErr := preparedActivityRetryTimer(ctx, workflow, activityID, retryTimerID, plan)
		if prepareErr != nil {
			return zero, prepareErr
		}
		if prepared {
			// A timer can commit before its first RETRYING ActivityRecord. It is
			// a durable prepare boundary: wait for it, then retry from the empty
			// authoritative attempt history if the activity write never landed.
			_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
			if time.Now().UTC().Before(wakeAt) {
				return zero, &TimerPendingError{
					TimerID: retryTimerID,
					WakeAt:  wakeAt,
				}
			}
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
				return zero, workflowInfrastructureError("create activity claim", createErr)
			}
			if created {
				claimAcquired = true
				break
			}

			// Bypass a cached miss: the winner may have committed a terminal
			// record between our initial read and failed conditional create.
			fresh, freshFound, freshErr := getActivityAuthoritative(ctx, workflow.store, key)
			if freshErr != nil {
				return zero, workflowInfrastructureError("refresh activity after claim race", freshErr)
			}
			if freshFound && fresh.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
				if activityRecordMayHaveRetryTimer(fresh) {
					_ = markActivityRetryTimerFired(ctx, workflow, activityID, fresh.GetRetryTimerId())
				}
				return replayRecord(fresh, activityType, workflow.codeVersion, newResult)
			}

			existing, claimFound, getErr := workflow.claimStore.GetClaim(ctx, activityClaimKey)
			if getErr != nil {
				return zero, workflowInfrastructureError("read competing activity claim", getErr)
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
			return zero, workflowInfrastructureError("refresh activity after claim acquisition", freshErr)
		}
		record, found = fresh, freshFound
		attempts = nil
		if found {
			switch record.GetStatus() {
			case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
				temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
				if activityRecordMayHaveRetryTimer(record) {
					_ = markActivityRetryTimerFired(ctx, workflow, activityID, record.GetRetryTimerId())
				}
				replayed, replayErr := replayRecord(record, activityType, workflow.codeVersion, newResult)
				return releaseActivityClaim(replayed, replayErr)
			case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
				if identityErr := assertActivityIdentity(record, activityType, workflow.codeVersion); identityErr != nil {
					return releaseActivityClaim(zero, identityErr)
				}
				if retryErr := assertRetryingActivity(record, normalizedRetryPolicy, retryTimerID, plan); retryErr != nil {
					return releaseActivityClaim(zero, retryErr)
				}
				if nextAt := record.GetNextAttemptAt(); nextAt != nil {
					wakeAt, ensureErr := ensureActivityRetryTimer(ctx, workflow, activityID, retryTimerID, record, plan)
					if ensureErr != nil {
						return releaseActivityClaim(zero, ensureErr)
					}
					_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
					if time.Now().UTC().Before(wakeAt) {
						return releaseActivityClaim(zero, &TimerPendingError{
							TimerID: retryTimerID,
							WakeAt:  wakeAt,
						})
					}
				} else if plan.durableThreshold > 0 {
					wakeAt, prepared, prepareErr := newerPreparedActivityRetryTimer(
						ctx,
						workflow,
						activityID,
						retryTimerID,
						record,
						plan,
					)
					if prepareErr != nil {
						return releaseActivityClaim(zero, prepareErr)
					}
					if prepared {
						_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
						if time.Now().UTC().Before(wakeAt) {
							return releaseActivityClaim(zero, &TimerPendingError{
								TimerID: retryTimerID,
								WakeAt:  wakeAt,
							})
						}
					}
				}
				attempts = record.GetAttempts()
			default:
				return releaseActivityClaim(
					zero,
					fmt.Errorf("%w: stored activity has unknown status", ErrActivityConflict),
				)
			}
		} else if plan.maxAttempts > 1 && plan.durableThreshold > 0 {
			// A prior holder can publish the timer-first durable boundary and
			// release its claim before its RETRYING ActivityRecord is visible.
			// The pre-claim check cannot cover that gap: re-read the prepared
			// timer authoritatively while holding the new claim before allowing
			// any activity execution.
			wakeAt, prepared, prepareErr := preparedActivityRetryTimer(
				ctx,
				workflow,
				activityID,
				retryTimerID,
				plan,
			)
			if prepareErr != nil {
				return releaseActivityClaim(zero, prepareErr)
			}
			if prepared {
				_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
				if time.Now().UTC().Before(wakeAt) {
					return releaseActivityClaim(zero, &TimerPendingError{
						TimerID: retryTimerID,
						WakeAt:  wakeAt,
					})
				}
			}
		}
	}

	if attempts == nil {
		attempts = make([]*temporalessv1.ActivityAttempt, 0, plan.maxAttempts)
	}
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
				RetryPolicy:   normalizedRetryPolicy,
				RetryTimerId:  retryTimerID,
			}
			if err := workflow.store.PutActivity(ctx, completedRecord); err != nil {
				// The terminal write may have committed despite the returned error.
				// Retain the claim rather than permit an unsafe automatic rerun.
				return zero, workflowInfrastructureError("persist completed activity", err)
			}
			if activityRecordMayHaveRetryTimer(completedRecord) {
				_ = markActivityRetryTimerFired(ctx, workflow, activityID, completedRecord.GetRetryTimerId())
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
				RetryPolicy:   normalizedRetryPolicy,
				RetryTimerId:  retryTimerID,
			}
			if err := workflow.store.PutActivity(ctx, failedRecord); err != nil {
				// Retain on an ambiguous terminal write.
				return zero, workflowInfrastructureError("persist failed activity", err)
			}
			if activityRecordMayHaveRetryTimer(failedRecord) {
				_ = markActivityRetryTimerFired(ctx, workflow, activityID, failedRecord.GetRetryTimerId())
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
			RetryPolicy:   normalizedRetryPolicy,
			RetryTimerId:  retryTimerID,
		}

		// The configured schedule is a pure function of the failed attempt
		// ordinal. Retry-After applies only to this failure's wait and never
		// becomes the exponential base for a later attempt or durable resume.
		interval, intervalErr := retryIntervalAfterAttempt(attemptIdx, plan)
		if intervalErr != nil {
			return zero, intervalErr
		}
		if ra := failure.GetRetryAfter().AsDuration(); ra > interval {
			interval = ra
		}

		// Durable retry branch: when the next backoff interval crosses the
		// configured threshold, publish the wake-bearing timer before the
		// RETRYING ActivityRecord. This prepare-first ordering prevents a process
		// death between the two independent point writes from leaving durable
		// retry state with no scheduler wake. If the activity write does not land,
		// replay recognizes the newer prepared timer and retries from the older
		// authoritative attempt history after that wake (at-least-once).
		if plan.durableThreshold > 0 && interval >= plan.durableThreshold {
			nextAttemptAt := time.Now().UTC().Add(interval)
			retryingRecord.NextAttemptAt = timestamppb.New(nextAttemptAt)
			effectiveWakeAt, timerErr := ensureActivityRetryTimer(ctx, workflow, activityID, retryTimerID, retryingRecord, plan)
			if timerErr != nil {
				// The activity body is a known failure. Release its claim even when
				// the timer write failed so replay can safely repeat the attempt.
				return releaseActivityClaim(zero, timerErr)
			}

			// The timer is now a verified durable prepare boundary. Release the
			// known-failed activity claim before any cancellation-sensitive activity
			// persistence or best-effort sleep reconciliation that follows.
			if _, releaseErr := releaseActivityClaim(zero, nil); releaseErr != nil {
				return zero, activityRetryTimerRepairPending(
					retryTimerID,
					effectiveWakeAt,
					"release activity claim after preparing retry timer",
					releaseErr,
				)
			}
			if err := workflow.store.PutActivity(ctx, retryingRecord); err != nil {
				return zero, activityRetryTimerRepairPending(
					retryTimerID,
					effectiveWakeAt,
					"persist RETRYING activity after preparing retry timer",
					err,
				)
			}
			// The paired retry timer is now a durable successor wake. It is safe
			// to retire any ordinary sleeps consumed earlier in this invocation.
			_ = workflow.acknowledgeConsumedSleepTimers(ctx, "")
			return zero, &TimerPendingError{
				TimerID: retryTimerID,
				WakeAt:  effectiveWakeAt,
			}
		}

		if err := workflow.store.PutActivity(ctx, retryingRecord); err != nil {
			// Retain on an ambiguous retry-state write.
			return zero, workflowInfrastructureError("persist retrying activity", err)
		}

		if err := sleepCtx(ctx, interval); err != nil {
			// The failed attempt and RETRYING record are durable. Releasing is
			// safe even when the caller canceled during the in-process backoff.
			return releaseActivityClaim(
				zero,
				workflowInfrastructureError("wait for activity retry backoff", err),
			)
		}
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
		if failure == nil {
			return zero, fmt.Errorf("%w: stored failed activity has no failure", ErrActivityConflict)
		}
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
		return retryPlan{maxAttempts: 1, backoffCoefficient: 1.0}, nil
	}
	initialInterval, err := retryPolicyDuration("initial_interval", policy.GetInitialInterval())
	if err != nil {
		return retryPlan{}, err
	}
	maximumInterval, err := retryPolicyDuration("maximum_interval", policy.GetMaximumInterval())
	if err != nil {
		return retryPlan{}, err
	}
	durableThreshold, err := retryPolicyDuration("durable_backoff_threshold", policy.GetDurableBackoffThreshold())
	if err != nil {
		return retryPlan{}, err
	}
	plan := retryPlan{
		maxAttempts:        policy.GetMaximumAttempts(),
		initialInterval:    initialInterval,
		backoffCoefficient: policy.GetBackoffCoefficient(),
		maximumInterval:    maximumInterval,
		durableThreshold:   durableThreshold,
	}
	if plan.maxAttempts == 0 {
		return retryPlan{}, fmt.Errorf("retry policy maximum_attempts must be > 0")
	}
	if plan.initialInterval < 0 {
		return retryPlan{}, fmt.Errorf("retry policy initial_interval must be >= 0")
	}
	if plan.maxAttempts > 1 && plan.initialInterval <= 0 {
		return retryPlan{}, fmt.Errorf("retry policy initial_interval must be > 0 when maximum_attempts > 1")
	}
	if plan.backoffCoefficient == 0 {
		plan.backoffCoefficient = 1.0
	}
	if math.IsNaN(plan.backoffCoefficient) || math.IsInf(plan.backoffCoefficient, 0) || plan.backoffCoefficient <= 0 {
		return retryPlan{}, fmt.Errorf("retry policy backoff_coefficient must be finite and > 0")
	}
	if plan.maximumInterval < 0 {
		return retryPlan{}, fmt.Errorf("retry policy maximum_interval must be >= 0")
	}
	if plan.maximumInterval > 0 && plan.initialInterval > plan.maximumInterval {
		return retryPlan{}, fmt.Errorf("retry policy maximum_interval must be >= initial_interval")
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

func retryPolicyDuration(name string, value *durationpb.Duration) (time.Duration, error) {
	if value == nil {
		return 0, nil
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("retry policy %s is invalid: %w", name, err)
	}
	duration := value.AsDuration()
	// google.protobuf.Duration has a much wider range than time.Duration.
	// Reject values that AsDuration would silently clamp instead of changing
	// the configured retry schedule.
	if !proto.Equal(value, durationpb.New(duration)) {
		return 0, fmt.Errorf("retry policy %s is outside Go time.Duration range", name)
	}
	return duration, nil
}

// normalizeRetryPolicy returns the canonical policy persisted with activity
// records. Nil is the canonical single-attempt policy. Explicit policies use
// their validated effective values, sort and de-duplicate set-like error
// codes, and omit zero duration messages so equivalent callers compare equal.
func normalizeRetryPolicy(policy *temporalessv1.RetryPolicy, plan retryPlan) *temporalessv1.RetryPolicy {
	if policy == nil {
		return nil
	}
	codes := append([]string(nil), policy.GetNonRetryableErrorCodes()...)
	sort.Strings(codes)
	uniqueCodes := codes[:0]
	for _, code := range codes {
		if len(uniqueCodes) == 0 || code != uniqueCodes[len(uniqueCodes)-1] {
			uniqueCodes = append(uniqueCodes, code)
		}
	}
	normalized := &temporalessv1.RetryPolicy{
		BackoffCoefficient:     plan.backoffCoefficient,
		MaximumAttempts:        plan.maxAttempts,
		NonRetryableErrorCodes: uniqueCodes,
	}
	if plan.initialInterval != 0 {
		normalized.InitialInterval = durationpb.New(plan.initialInterval)
	}
	if plan.maximumInterval != 0 {
		normalized.MaximumInterval = durationpb.New(plan.maximumInterval)
	}
	if plan.durableThreshold != 0 {
		normalized.DurableBackoffThreshold = durationpb.New(plan.durableThreshold)
	}
	return normalized
}

func assertRetryingActivity(
	record *temporalessv1.ActivityRecord,
	normalizedPolicy *temporalessv1.RetryPolicy,
	retryTimerID string,
	plan retryPlan,
) error {
	if !proto.Equal(record.GetRetryPolicy(), normalizedPolicy) {
		return fmt.Errorf("%w: retry policy changed while activity is RETRYING", ErrActivityConflict)
	}
	attempts := record.GetAttempts()
	if len(attempts) == 0 {
		return fmt.Errorf("%w: RETRYING activity has no attempts", ErrActivityConflict)
	}
	if uint64(len(attempts)) >= uint64(plan.maxAttempts) {
		return fmt.Errorf(
			"%w: RETRYING activity has %d attempts under a %d-attempt policy",
			ErrActivityConflict,
			len(attempts),
			plan.maxAttempts,
		)
	}
	for index, attempt := range attempts {
		wantAttempt := uint32(index + 1)
		if attempt == nil || attempt.GetAttempt() != wantAttempt {
			return fmt.Errorf("%w: RETRYING activity attempt %d is missing or out of sequence", ErrActivityConflict, wantAttempt)
		}
		if attempt.GetFailure() == nil {
			return fmt.Errorf("%w: RETRYING activity attempt %d has no failure", ErrActivityConflict, wantAttempt)
		}
		retryAfter, err := retryPolicyDuration("persisted retry_after", attempt.GetFailure().GetRetryAfter())
		if err != nil {
			return fmt.Errorf("%w: %w", ErrActivityConflict, err)
		}
		if retryAfter < 0 {
			return fmt.Errorf("%w: RETRYING activity attempt %d has negative retry_after", ErrActivityConflict, wantAttempt)
		}
	}
	lastAttempt := attempts[len(attempts)-1]
	lastFailure := lastAttempt.GetFailure()
	if record.GetFailure() == nil || !proto.Equal(record.GetFailure(), lastFailure) {
		return fmt.Errorf("%w: RETRYING activity failure does not match its last attempt", ErrActivityConflict)
	}
	if plan.nonRetryable[lastFailure.GetCode()] {
		return fmt.Errorf(
			"%w: RETRYING activity last failure code %q is non-retryable",
			ErrActivityConflict,
			lastFailure.GetCode(),
		)
	}
	effectiveInterval, intervalErr := retryIntervalAfterAttempt(lastAttempt.GetAttempt(), plan)
	if intervalErr != nil {
		return fmt.Errorf("%w: reconstruct RETRYING activity interval: %w", ErrActivityConflict, intervalErr)
	}
	if retryAfter := lastFailure.GetRetryAfter().AsDuration(); retryAfter > effectiveInterval {
		effectiveInterval = retryAfter
	}
	durableExpected := plan.durableThreshold > 0 && effectiveInterval >= plan.durableThreshold
	if durableExpected && record.GetNextAttemptAt() == nil {
		return fmt.Errorf(
			"%w: RETRYING activity interval %s requires next_attempt_at and a durable retry timer",
			ErrActivityConflict,
			effectiveInterval,
		)
	}
	if !durableExpected && record.GetNextAttemptAt() != nil {
		return fmt.Errorf(
			"%w: RETRYING activity has next_attempt_at for non-durable interval %s",
			ErrActivityConflict,
			effectiveInterval,
		)
	}
	if record.GetRetryTimerId() != retryTimerID {
		return fmt.Errorf(
			"%w: retry timer ID changed from %q to %q while activity is RETRYING",
			ErrActivityConflict,
			record.GetRetryTimerId(),
			retryTimerID,
		)
	}
	if nextAt := record.GetNextAttemptAt(); nextAt != nil {
		if err := nextAt.CheckValid(); err != nil {
			return fmt.Errorf("%w: RETRYING activity next_attempt_at is invalid: %w", ErrActivityConflict, err)
		}
	}
	return nil
}

// retryIntervalAfterAttempt computes the policy delay after a failed attempt
// by ordinal. It is deliberately independent of any prior effective delay:
// a vendor Retry-After affects only its own attempt and is never compounded.
func retryIntervalAfterAttempt(attempt uint32, plan retryPlan) (time.Duration, error) {
	if attempt == 0 {
		return 0, fmt.Errorf("retry attempt ordinal must be > 0")
	}
	interval := plan.initialInterval
	for ordinal := uint32(1); ordinal < attempt; ordinal++ {
		next, err := nextInterval(interval, plan)
		if err != nil {
			return 0, err
		}
		interval = next
	}
	return interval, nil
}

// putActivityRetryTimer writes (or overwrites) the TIMER_KIND_ACTIVITY_RETRY
// timer paired with an activity's durable retry. The caller supplies a stable
// ID so later retries overwrite the same record.
func putActivityRetryTimer(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
	duration time.Duration,
	fireAt time.Time,
) error {
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    retryTimerID,
	}
	record := &temporalessv1.TimerRecord{
		SchemaVersion:   storage.TimerRecordSchemaVersion,
		Key:             key.Proto(),
		TimerKind:       temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
		CodeVersion:     workflow.codeVersion,
		Duration:        durationpb.New(duration),
		Status:          temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:          timestamppb.New(fireAt),
		CreatedAt:       timestamppb.New(time.Now().UTC()),
		RetryActivityId: activityID,
	}
	return workflow.store.PutTimer(ctx, record)
}

// ensureActivityRetryTimer closes the non-transactional boundary between the
// authoritative RETRYING ActivityRecord and its wake-bearing TimerRecord.
// Missing timers and compatible stale prior retry timers are repaired from the
// persisted attempt/policy schedule. A reserved-ID collision or corrupt timer
// is rejected rather than overwritten.
func ensureActivityRetryTimer(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
	retrying *temporalessv1.ActivityRecord,
	plan retryPlan,
) (time.Time, error) {
	if retrying == nil || retrying.GetRetryTimerId() != retryTimerID {
		return time.Time{}, fmt.Errorf(
			"%w: RETRYING activity retry_timer_id %q does not match requested timer %q",
			ErrActivityConflict,
			retrying.GetRetryTimerId(),
			retryTimerID,
		)
	}
	wakeAt, duration, err := activityRetryTimerExpectation(retrying, plan)
	if err != nil {
		return time.Time{}, err
	}
	timerID := retryTimerID
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    timerID,
	}
	record, found, err := getTimerAuthoritative(ctx, workflow.store, key)
	if err != nil {
		return wakeAt, activityRetryTimerRepairPending(timerID, wakeAt, "read paired retry timer", err)
	}
	if !found {
		return wakeAt, putActivityRetryTimerVerified(
			ctx,
			workflow,
			activityID,
			retryTimerID,
			duration,
			wakeAt,
			"create missing paired retry timer",
		)
	}
	storedWakeAt, storedDuration, status, err := activityRetryTimerDetails(record, key, workflow.codeVersion, activityID)
	if err != nil {
		return wakeAt, err
	}
	if storedWakeAt.After(wakeAt) {
		if err := validateNewerPreparedActivityRetryTimer(retrying, plan, storedDuration); err != nil {
			return storedWakeAt, err
		}
		if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
			if err := putActivityRetryTimerVerified(
				ctx,
				workflow,
				activityID,
				retryTimerID,
				storedDuration,
				storedWakeAt,
				"re-arm newer prepared retry timer",
			); err != nil {
				return storedWakeAt, err
			}
		}
		// Timer-first persistence intentionally permits a wake newer than a
		// lagging ActivityRecord. Never regress this prepared boundary.
		return storedWakeAt, nil
	}
	if storedWakeAt.Equal(wakeAt) && storedDuration != duration {
		return wakeAt, fmt.Errorf(
			"%w: activity retry timer %q duration %s does not match retry policy duration %s",
			ErrTimerConflict,
			timerID,
			storedDuration,
			duration,
		)
	}
	if storedWakeAt.Equal(wakeAt) && status == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		return wakeAt, nil
	}

	// A valid earlier schedule is stale state from a prior retry. A compatible
	// same-wake FIRED record can also come from an older runtime that consumed
	// the wake before committing its activity result. Repair both forward to the
	// ActivityRecord's authoritative boundary.
	return wakeAt, putActivityRetryTimerVerified(
		ctx,
		workflow,
		activityID,
		retryTimerID,
		duration,
		wakeAt,
		"repair stale paired retry timer",
	)
}

func activityRetryTimerDetails(
	record *temporalessv1.TimerRecord,
	key storage.TimerKey,
	codeVersion string,
	activityID string,
) (time.Time, time.Duration, temporalessv1.TimerStatus, error) {
	timerID := key.TimerID
	if record.GetSchemaVersion() != storage.TimerRecordSchemaVersion {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has schema version %s", ErrTimerConflict, timerID, record.GetSchemaVersion())
	}
	if record.GetKey() == nil || storage.TimerKeyFromProto(record.GetKey()) != key {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has a conflicting embedded key", ErrTimerConflict, timerID)
	}
	if record.GetTimerKind() != temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: reserved activity retry timer %q has kind %s", ErrTimerConflict, timerID, record.GetTimerKind())
	}
	if record.GetRetryActivityId() != activityID {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: activity retry timer %q belongs to activity %q, not %q",
			ErrTimerConflict,
			timerID,
			record.GetRetryActivityId(),
			activityID,
		)
	}
	if record.GetCodeVersion() != codeVersion {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf(
			"%w: activity retry timer %q code version changed from %q to %q",
			ErrTimerConflict,
			timerID,
			record.GetCodeVersion(),
			codeVersion,
		)
	}
	if record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q was canceled", ErrTimerConflict, timerID)
	}
	if record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED &&
		record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has invalid status %s", ErrTimerConflict, timerID, record.GetStatus())
	}
	if record.GetFireAt() == nil {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has no fire_at", ErrTimerConflict, timerID)
	}
	if err := record.GetFireAt().CheckValid(); err != nil {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has invalid fire_at: %w", ErrTimerConflict, timerID, err)
	}
	storedDuration, err := activityRetryTimerDuration(record.GetDuration())
	if err != nil {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q %w", ErrTimerConflict, timerID, err)
	}
	if record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED && record.GetFiredAt() != nil {
		return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: scheduled activity retry timer %q has fired_at", ErrTimerConflict, timerID)
	}
	if record.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		if record.GetFiredAt() == nil {
			return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: fired activity retry timer %q has no fired_at", ErrTimerConflict, timerID)
		}
		if err := record.GetFiredAt().CheckValid(); err != nil {
			return time.Time{}, 0, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED, fmt.Errorf("%w: activity retry timer %q has invalid fired_at: %w", ErrTimerConflict, timerID, err)
		}
	}
	return record.GetFireAt().AsTime(), storedDuration, record.GetStatus(), nil
}

func inspectActivityRetryTimer(
	record *temporalessv1.TimerRecord,
	key storage.TimerKey,
	codeVersion string,
	activityID string,
	duration time.Duration,
	wakeAt time.Time,
) (bool, error) {
	storedWakeAt, storedDuration, status, err := activityRetryTimerDetails(record, key, codeVersion, activityID)
	if err != nil {
		return false, err
	}
	return status == temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED &&
		storedWakeAt.Equal(wakeAt) && storedDuration == duration, nil
}

func validateNewerPreparedActivityRetryTimer(
	retrying *temporalessv1.ActivityRecord,
	plan retryPlan,
	duration time.Duration,
) error {
	attempts := retrying.GetAttempts()
	if len(attempts) == 0 || attempts[len(attempts)-1] == nil {
		return fmt.Errorf("%w: cannot validate a newer retry timer without authoritative attempts", ErrTimerConflict)
	}
	preparedAfterAttempt := attempts[len(attempts)-1].GetAttempt() + 1
	if preparedAfterAttempt >= plan.maxAttempts {
		return fmt.Errorf(
			"%w: newer retry timer would follow terminal attempt %d",
			ErrTimerConflict,
			preparedAfterAttempt,
		)
	}
	minimum, err := retryIntervalAfterAttempt(preparedAfterAttempt, plan)
	if err != nil {
		return fmt.Errorf("%w: validate newer prepared retry interval: %w", ErrTimerConflict, err)
	}
	if duration < minimum || duration < plan.durableThreshold {
		return fmt.Errorf(
			"%w: newer prepared retry timer duration %s is below required interval %s",
			ErrTimerConflict,
			duration,
			minimum,
		)
	}
	return nil
}

// preparedActivityRetryTimer recognizes the timer-first half of the first
// durable retry boundary, where no ActivityRecord was committed. The timer's
// full duration must be compatible with a retry after attempt one. Once due,
// execution repeats attempt one from empty authoritative history.
func preparedActivityRetryTimer(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
	plan retryPlan,
) (time.Time, bool, error) {
	timerID := retryTimerID
	key := storage.NewTimerKey(workflow.workflowID, workflow.runID, timerID)
	record, found, err := getTimerAuthoritative(ctx, workflow.store, key)
	if err != nil {
		wakeAt := time.Now().UTC()
		return wakeAt, false, activityRetryTimerRepairPending(timerID, wakeAt, "read prepared retry timer", err)
	}
	if !found {
		return time.Time{}, false, nil
	}
	wakeAt, duration, status, err := activityRetryTimerDetails(record, key, workflow.codeVersion, activityID)
	if err != nil {
		return time.Time{}, false, err
	}
	minimum, err := retryIntervalAfterAttempt(1, plan)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("%w: validate prepared retry interval: %w", ErrTimerConflict, err)
	}
	if duration < minimum || duration < plan.durableThreshold {
		return time.Time{}, false, fmt.Errorf(
			"%w: prepared retry timer duration %s is below required interval %s",
			ErrTimerConflict,
			duration,
			minimum,
		)
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		if err := putActivityRetryTimerVerified(
			ctx,
			workflow,
			activityID,
			retryTimerID,
			duration,
			wakeAt,
			"re-arm prepared retry timer",
		); err != nil {
			return wakeAt, false, err
		}
	}
	return wakeAt, true, nil
}

// newerPreparedActivityRetryTimer recognizes the timer-first half of a later
// durable retry boundary when the authoritative ActivityRecord still describes
// an earlier in-process retry. That older record has no next_attempt_at, so
// ensureActivityRetryTimer cannot derive the newer wake from it; the timer's
// duration and reciprocal activity link prove the compatible forward boundary.
func newerPreparedActivityRetryTimer(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
	retrying *temporalessv1.ActivityRecord,
	plan retryPlan,
) (time.Time, bool, error) {
	key := storage.NewTimerKey(workflow.workflowID, workflow.runID, retryTimerID)
	record, found, err := getTimerAuthoritative(ctx, workflow.store, key)
	if err != nil {
		wakeAt := time.Now().UTC()
		return wakeAt, false, activityRetryTimerRepairPending(retryTimerID, wakeAt, "read newer prepared retry timer", err)
	}
	if !found {
		return time.Time{}, false, nil
	}
	wakeAt, duration, status, err := activityRetryTimerDetails(record, key, workflow.codeVersion, activityID)
	if err != nil {
		return time.Time{}, false, err
	}
	if err := validateNewerPreparedActivityRetryTimer(retrying, plan, duration); err != nil {
		return time.Time{}, false, err
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		if err := putActivityRetryTimerVerified(
			ctx,
			workflow,
			activityID,
			retryTimerID,
			duration,
			wakeAt,
			"re-arm newer prepared retry timer",
		); err != nil {
			return wakeAt, false, err
		}
	}
	return wakeAt, true, nil
}

// putActivityRetryTimerVerified handles an ambiguous PutTimer error by reading
// the authoritative point record back. Some remote stores can commit a write
// and still return an error after the response path fails; an exact re-read is
// therefore proof of durability. A missing or stale record remains pending so
// replay can repair it, while an incompatible record is a hard conflict.
func putActivityRetryTimerVerified(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
	duration time.Duration,
	wakeAt time.Time,
	operation string,
) error {
	writeErr := putActivityRetryTimer(ctx, workflow, activityID, retryTimerID, duration, wakeAt)
	if writeErr == nil {
		return nil
	}

	timerID := retryTimerID
	key := storage.NewTimerKey(workflow.workflowID, workflow.runID, timerID)
	verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	record, found, readErr := getTimerAuthoritative(verifyCtx, workflow.store, key)
	if readErr != nil {
		return activityRetryTimerRepairPending(
			timerID,
			wakeAt,
			operation,
			errors.Join(writeErr, fmt.Errorf("verify ambiguous timer write: %w", readErr)),
		)
	}
	if found {
		exact, inspectErr := inspectActivityRetryTimer(record, key, workflow.codeVersion, activityID, duration, wakeAt)
		if inspectErr != nil {
			return errors.Join(inspectErr, fmt.Errorf("%s %q: %w", operation, timerID, writeErr))
		}
		if exact {
			return nil
		}
	}
	return activityRetryTimerRepairPending(timerID, wakeAt, operation, writeErr)
}

func activityRetryTimerExpectation(
	retrying *temporalessv1.ActivityRecord,
	plan retryPlan,
) (time.Time, time.Duration, error) {
	if retrying == nil || retrying.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		return time.Time{}, 0, fmt.Errorf("%w: retry timer requires a RETRYING activity record", ErrActivityConflict)
	}
	if retrying.GetNextAttemptAt() == nil {
		return time.Time{}, 0, fmt.Errorf("%w: durable RETRYING activity has no next_attempt_at", ErrActivityConflict)
	}
	if plan.durableThreshold <= 0 {
		return time.Time{}, 0, fmt.Errorf("%w: activity has next_attempt_at under a non-durable retry policy", ErrActivityConflict)
	}
	attempts := retrying.GetAttempts()
	if len(attempts) == 0 || attempts[len(attempts)-1] == nil || attempts[len(attempts)-1].GetFailure() == nil {
		return time.Time{}, 0, fmt.Errorf("%w: durable RETRYING activity has no failed attempt", ErrActivityConflict)
	}
	lastAttempt := attempts[len(attempts)-1]
	if retrying.GetFailure() == nil || !proto.Equal(retrying.GetFailure(), lastAttempt.GetFailure()) {
		return time.Time{}, 0, fmt.Errorf("%w: RETRYING activity failure does not match its last attempt", ErrActivityConflict)
	}
	duration, err := retryIntervalAfterAttempt(lastAttempt.GetAttempt(), plan)
	if err != nil {
		return time.Time{}, 0, fmt.Errorf("%w: reconstruct activity retry interval: %w", ErrActivityConflict, err)
	}
	if retryAfter := lastAttempt.GetFailure().GetRetryAfter().AsDuration(); retryAfter > duration {
		duration = retryAfter
	}
	if duration < plan.durableThreshold {
		return time.Time{}, 0, fmt.Errorf(
			"%w: activity next_attempt_at interval %s is below durable threshold %s",
			ErrActivityConflict,
			duration,
			plan.durableThreshold,
		)
	}
	return retrying.GetNextAttemptAt().AsTime(), duration, nil
}

func activityRetryTimerDuration(value *durationpb.Duration) (time.Duration, error) {
	if value == nil {
		return 0, fmt.Errorf("has no duration")
	}
	if err := value.CheckValid(); err != nil {
		return 0, fmt.Errorf("has invalid duration: %w", err)
	}
	duration := value.AsDuration()
	if duration <= 0 || !proto.Equal(value, durationpb.New(duration)) {
		return 0, fmt.Errorf("has invalid duration %s", value)
	}
	return duration, nil
}

func activityRetryTimerRepairPending(timerID string, wakeAt time.Time, operation string, cause error) error {
	infrastructureErr := workflowInfrastructureError(
		fmt.Sprintf("%s %q", operation, timerID),
		cause,
	)
	if !errors.Is(infrastructureErr, ErrWorkflowInfrastructure) {
		return infrastructureErr
	}
	return errors.Join(
		&TimerPendingError{TimerID: timerID, WakeAt: wakeAt},
		infrastructureErr,
	)
}

// activityRecordMayHaveRetryTimer reports whether the activity policy can
// publish durable retry timers. With timer-first persistence, a prior failed
// attempt and its RETRYING write can be lost while the prepared timer remains;
// terminal attempt history alone therefore cannot prove that no timer exists.
func activityRecordMayHaveRetryTimer(record *temporalessv1.ActivityRecord) bool {
	if record.GetRetryPolicy() == nil {
		return false
	}
	plan, err := planRetries(record.GetRetryPolicy())
	if err != nil {
		// A malformed terminal audit policy must not invalidate the terminal
		// result, but conservatively try to clean up a possible wakeup.
		return true
	}
	return plan.maxAttempts > 1 && plan.durableThreshold > 0
}

// markActivityRetryTimerFired transitions the paired retry timer only after a
// terminal ActivityRecord is durable. Keeping it SCHEDULED while the resumed
// body runs prevents a process crash from creating a lost-wakeup window.
// Callers treat cleanup as best-effort: the terminal activity result/failure
// remains authoritative, terminal replay retries reconciliation, and the due
// ledger also prunes entries whose parent workflow is terminal.
func markActivityRetryTimerFired(
	ctx context.Context,
	workflow *Workflow,
	activityID string,
	retryTimerID string,
) error {
	if retryTimerID == "" {
		return nil
	}
	timerID := retryTimerID
	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: workflow.workflowID,
		RunID:      workflow.runID,
		TimerID:    timerID,
	}
	record, found, err := getTimerAuthoritative(ctx, workflow.store, key)
	if err != nil {
		return fmt.Errorf("get activity retry timer %q for terminal cleanup: %w", timerID, err)
	}
	if !found {
		return nil
	}
	_, _, status, err := activityRetryTimerDetails(record, key, workflow.codeVersion, activityID)
	if err != nil {
		return fmt.Errorf("validate activity retry timer %q for terminal cleanup: %w", timerID, err)
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		return nil
	}
	updated := proto.Clone(record).(*temporalessv1.TimerRecord)
	updated.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
	updated.FiredAt = timestamppb.New(time.Now().UTC())
	if err := workflow.store.PutTimer(ctx, updated); err != nil {
		return fmt.Errorf("finalize activity retry timer %q: %w", timerID, err)
	}
	return nil
}

func nextInterval(prev time.Duration, plan retryPlan) (time.Duration, error) {
	nextNanos := float64(prev) * plan.backoffCoefficient
	if plan.maximumInterval > 0 && nextNanos >= float64(plan.maximumInterval) {
		return plan.maximumInterval, nil
	}
	if math.IsNaN(nextNanos) || math.IsInf(nextNanos, 0) || nextNanos > float64(math.MaxInt64) {
		return 0, fmt.Errorf("retry policy produced an out-of-range backoff interval")
	}
	next := time.Duration(nextNanos)
	if prev > 0 && next <= 0 {
		return 0, fmt.Errorf("retry policy produced a non-positive backoff interval")
	}
	return next, nil
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
	runOrderTime *timestamppb.Timestamp,
	newResult func() T,
) (T, error) {
	var zero T
	if err := assertWorkflowIdentity(record, workflowType, codeVersion, runOrderTime); err != nil {
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
		if failure == nil {
			return zero, fmt.Errorf("%w: stored failed workflow has no failure", ErrWorkflowConflict)
		}
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
	runOrderTime *timestamppb.Timestamp,
) error {
	if record.GetWorkflowType() != workflowType {
		return fmt.Errorf("%w: workflow type changed from %q to %q", ErrWorkflowConflict, record.GetWorkflowType(), workflowType)
	}
	if record.GetCodeVersion() != codeVersion {
		return fmt.Errorf("%w: code version changed from %q to %q", ErrWorkflowConflict, record.GetCodeVersion(), codeVersion)
	}
	if stored := record.GetRunOrderTime(); stored != nil {
		if err := stored.CheckValid(); err != nil {
			return fmt.Errorf("%w: stored run_order_time is invalid: %w", ErrWorkflowConflict, err)
		}
	}
	if runOrderTime != nil {
		if err := runOrderTime.CheckValid(); err != nil {
			return fmt.Errorf("%w: requested run_order_time is invalid: %w", ErrWorkflowConflict, err)
		}
	}
	if !proto.Equal(record.GetRunOrderTime(), runOrderTime) {
		return fmt.Errorf("%w: run_order_time changed for workflow run", ErrWorkflowConflict)
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
	if normalized.GetRunOrderTime() != nil {
		if err := normalized.GetRunOrderTime().CheckValid(); err != nil {
			return nil, fmt.Errorf("workflow run_order_time is invalid: %w", err)
		}
	}
	if err := protovalidate.Validate(normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}
