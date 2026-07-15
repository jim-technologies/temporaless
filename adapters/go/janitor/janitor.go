// Package janitor sweeps completed workflow runs older than a max-age threshold
// and recursively deletes every run-scoped record, including claims when the
// configured claim store supports bounded run listing.
//
// Sweep is a multi-step retention operation, not a transaction or execution
// fence. Eligible runs must be externally quiesced before the janitor executes.
package janitor

import (
	"context"
	"errors"
	"fmt"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

var (
	// ErrClaimRunListingUnsupported means a claim-capable store cannot enumerate
	// one run. Sweeping is rejected before mutation rather than leaking claims.
	ErrClaimRunListingUnsupported = errors.New("claim store does not support run-scoped claim listing")
	// ErrRunListingDataLoss means a bounded listing returned an invalid key or a
	// payload whose embedded key belongs to another workflow run.
	ErrRunListingDataLoss = errors.New("run listing contains invalid or misplaced record key")
)

type runDeletionPlan struct {
	key        storage.WorkflowKey
	claims     []*temporalessv1.ClaimRecord
	activities []*temporalessv1.ActivityRecord
	timers     []*temporalessv1.TimerRecord
	events     []*temporalessv1.EventRecord
}

// Sweep deletes every COMPLETED workflow run selected by request. Claims,
// activities, timers, and events are deleted before the workflow record.
//
// claimStore may be nil. In that case Sweep uses store itself when it implements
// storage.ClaimStore; otherwise retention runs in explicit no-claims mode. A
// separately configured claim store takes precedence when provided.
//
// Sweep preflights claim capability and validates every eligible run snapshot
// before the first mutation. It stops on the first deletion error and reports
// the number of fully deleted runs before that error.
func Sweep(
	ctx context.Context,
	query storage.WorkflowQueryStore,
	store storage.Store,
	claimStore storage.ClaimStore,
	request *temporalessv1.SweepRequest,
) (uint32, error) {
	if query == nil {
		return 0, fmt.Errorf("query store is required")
	}
	if store == nil {
		return 0, fmt.Errorf("store is required")
	}
	if request == nil {
		return 0, fmt.Errorf("sweep request is required")
	}
	if request.GetNow() == nil {
		return 0, fmt.Errorf("now is required")
	}
	if err := request.GetNow().CheckValid(); err != nil {
		return 0, fmt.Errorf("now is invalid: %w", err)
	}
	if request.GetMaxAge() == nil || request.GetMaxAge().CheckValid() != nil || request.GetMaxAge().AsDuration() <= 0 {
		return 0, fmt.Errorf("max_age must be > 0")
	}

	claimStore, claimRunStore, err := preflightClaimStore(ctx, store, claimStore)
	if err != nil {
		return 0, err
	}

	var completed []*temporalessv1.WorkflowRecord
	pageToken := ""
	seenPageTokens := map[string]struct{}{}
	for {
		response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
			Namespace: request.GetNamespace(),
			Status:    temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			PageToken: pageToken,
		})
		if err != nil {
			return 0, err
		}
		completed = append(completed, response.GetRecords()...)
		next := response.GetNextPageToken()
		if next == "" {
			break
		}
		if _, repeated := seenPageTokens[next]; repeated {
			return 0, fmt.Errorf("retention query repeated page token %q", next)
		}
		seenPageTokens[next] = struct{}{}
		pageToken = next
	}

	cutoff := request.GetNow().AsTime().Add(-request.GetMaxAge().AsDuration())
	plans := make([]runDeletionPlan, 0, len(completed))
	seenRuns := make(map[storage.WorkflowKey]struct{}, len(completed))
	for _, record := range completed {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if record == nil {
			return 0, fmt.Errorf("%w: query returned a nil workflow record", ErrRunListingDataLoss)
		}

		key := storage.WorkflowKeyFromProto(record.GetKey())
		if err := validateWorkflowCandidate(request.GetNamespace(), key); err != nil {
			return 0, err
		}
		if key.Namespace == "" {
			key.Namespace = storage.DefaultNamespace
		}
		if _, duplicate := seenRuns[key]; duplicate {
			return 0, fmt.Errorf("%w: query returned duplicate workflow run", ErrRunListingDataLoss)
		}
		seenRuns[key] = struct{}{}
		// The query is only a candidate source and may lag an authoritative state
		// transition. Re-read the point record before snapshotting any children so
		// a stale COMPLETED row cannot delete a reset or resumed run.
		authoritative, found, err := store.GetWorkflow(ctx, key)
		if err != nil {
			return 0, fmt.Errorf("read authoritative workflow %s/%s: %w", key.WorkflowID, key.RunID, err)
		}
		if !found {
			continue
		}
		if err := storage.ValidateWorkflowRecord(authoritative, key); err != nil {
			return 0, fmt.Errorf("validate authoritative workflow %s/%s: %w", key.WorkflowID, key.RunID, err)
		}
		if authoritative.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
			continue
		}
		completedAt := authoritative.GetCompletedAt()
		if completedAt == nil || completedAt.CheckValid() != nil {
			return 0, fmt.Errorf("%w: completed workflow has invalid completed_at", ErrRunListingDataLoss)
		}
		if completedAt.AsTime().After(cutoff) {
			continue
		}
		plan, err := snapshotRun(ctx, store, claimRunStore, key)
		if err != nil {
			return 0, fmt.Errorf("snapshot run %s/%s: %w", key.WorkflowID, key.RunID, err)
		}
		plans = append(plans, plan)
	}

	var deleted uint32
	for _, plan := range plans {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := deleteRun(ctx, store, claimStore, plan); err != nil {
			return deleted, fmt.Errorf("delete run %s/%s: %w", plan.key.WorkflowID, plan.key.RunID, err)
		}
		deleted++
	}
	return deleted, nil
}

func preflightClaimStore(
	ctx context.Context,
	store storage.Store,
	claimStore storage.ClaimStore,
) (storage.ClaimStore, storage.ClaimRunStore, error) {
	if claimStore == nil {
		claimStore, _ = store.(storage.ClaimStore)
	}
	if claimStore == nil {
		return nil, nil, nil
	}
	capability, err := claimStore.ClaimCapability(ctx)
	if err != nil {
		return nil, nil, err
	}
	if capability != storage.CreateOnlyClaims && capability != storage.CASClaims {
		return claimStore, nil, nil
	}
	claimRunStore, ok := claimStore.(storage.ClaimRunStore)
	if !ok {
		return nil, nil, ErrClaimRunListingUnsupported
	}
	return claimStore, claimRunStore, nil
}

func validateWorkflowCandidate(namespace string, key storage.WorkflowKey) error {
	if err := key.Validate(); err != nil {
		return fmt.Errorf("%w: invalid workflow key in sweep listing: %w", ErrRunListingDataLoss, err)
	}
	if namespace == "" {
		return nil
	}
	targetNamespace := namespace
	if targetNamespace == "" {
		targetNamespace = storage.DefaultNamespace
	}
	recordNamespace := key.Namespace
	if recordNamespace == "" {
		recordNamespace = storage.DefaultNamespace
	}
	if recordNamespace != targetNamespace {
		return fmt.Errorf("%w: workflow payload key does not match requested namespace", ErrRunListingDataLoss)
	}
	return nil
}

func snapshotRun(
	ctx context.Context,
	store storage.Store,
	claimStore storage.ClaimRunStore,
	key storage.WorkflowKey,
) (runDeletionPlan, error) {
	plan := runDeletionPlan{key: key}
	var err error
	if claimStore != nil {
		plan.claims, err = claimStore.ListClaims(ctx, key)
		if err != nil {
			return runDeletionPlan{}, err
		}
	}
	plan.activities, err = store.ListActivities(ctx, key)
	if err != nil {
		return runDeletionPlan{}, err
	}
	plan.timers, err = store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		return runDeletionPlan{}, err
	}
	plan.events, err = store.ListEvents(ctx, key)
	if err != nil {
		return runDeletionPlan{}, err
	}

	for _, record := range plan.claims {
		claimKey := storage.ClaimKeyFromProto(record.GetKey())
		if err := validateListedRunRecordKey(
			"claim", key,
			claimKey.Namespace, claimKey.WorkflowID, claimKey.RunID,
			claimKey.Validate(),
		); err != nil {
			return runDeletionPlan{}, err
		}
	}
	for _, record := range plan.activities {
		activityKey := storage.ActivityKeyFromProto(record.GetKey())
		if err := validateListedRunRecordKey(
			"activity", key,
			activityKey.Namespace, activityKey.WorkflowID, activityKey.RunID,
			activityKey.Validate(),
		); err != nil {
			return runDeletionPlan{}, err
		}
	}
	for _, record := range plan.timers {
		timerKey := storage.TimerKeyFromProto(record.GetKey())
		if err := validateListedRunRecordKey(
			"timer", key,
			timerKey.Namespace, timerKey.WorkflowID, timerKey.RunID,
			timerKey.Validate(),
		); err != nil {
			return runDeletionPlan{}, err
		}
	}
	for _, record := range plan.events {
		eventKey := storage.EventKeyFromProto(record.GetKey())
		if err := validateListedRunRecordKey(
			"event", key,
			eventKey.Namespace, eventKey.WorkflowID, eventKey.RunID,
			eventKey.Validate(),
		); err != nil {
			return runDeletionPlan{}, err
		}
	}
	return plan, nil
}

func validateListedRunRecordKey(
	recordKind string,
	target storage.WorkflowKey,
	recordNamespace string,
	recordWorkflowID string,
	recordRunID string,
	validateErr error,
) error {
	if validateErr != nil {
		return fmt.Errorf("%w: invalid %s key in run listing: %w", ErrRunListingDataLoss, recordKind, validateErr)
	}
	targetNamespace := target.Namespace
	if targetNamespace == "" {
		targetNamespace = storage.DefaultNamespace
	}
	if recordNamespace == "" {
		recordNamespace = storage.DefaultNamespace
	}
	if recordNamespace != targetNamespace || recordWorkflowID != target.WorkflowID || recordRunID != target.RunID {
		return fmt.Errorf("%w: %s payload key does not match requested workflow run", ErrRunListingDataLoss, recordKind)
	}
	return nil
}

func deleteRun(
	ctx context.Context,
	store storage.Store,
	claimStore storage.ClaimStore,
	plan runDeletionPlan,
) error {
	if claimStore != nil {
		for _, record := range plan.claims {
			if _, err := claimStore.DeleteClaim(ctx, storage.ClaimKeyFromProto(record.GetKey())); err != nil {
				return err
			}
		}
	}
	for _, record := range plan.activities {
		if _, err := store.DeleteActivity(ctx, storage.ActivityKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	for _, record := range plan.timers {
		if _, err := store.DeleteTimer(ctx, storage.TimerKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	for _, record := range plan.events {
		if _, err := store.DeleteEvent(ctx, storage.EventKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	if _, err := store.DeleteWorkflow(ctx, plan.key); err != nil {
		return err
	}
	return nil
}
