package cronscheduler

import (
	"context"
	"fmt"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// LastFireFromRuns scans existing workflow records for `scheduleID` and
// returns the most recent fire time, parsed from run_ids using `runIDLayout`
// (e.g. `time.RFC3339`).
//
// This is the recommended path to seed the scheduler statelessly: when run_ids
// embed the schedule fire time, the storage tree already carries the
// scheduler's "memory". No separate persistence needed.
//
// Returns the zero `time.Time` and ok=false when no parseable runs exist yet.
// Run records whose IDs do not parse with `runIDLayout` are skipped.
func LastFireFromRuns(
	ctx context.Context,
	store storage.Store,
	namespace string,
	scheduleID string,
	runIDLayout string,
) (time.Time, bool, error) {
	if store == nil {
		return time.Time{}, false, fmt.Errorf("store is required")
	}
	if scheduleID == "" {
		return time.Time{}, false, fmt.Errorf("scheduleID is required")
	}
	if runIDLayout == "" {
		return time.Time{}, false, fmt.Errorf("runIDLayout is required (e.g. time.RFC3339)")
	}

	// Use the workflow_id filter so the storage walk is scoped to this
	// schedule's runs only — O(runs) instead of O(all workflows).
	records, err := store.ListWorkflows(ctx, namespace, scheduleID, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		return time.Time{}, false, err
	}

	var latest time.Time
	found := false
	for _, record := range records {
		fireTime, err := time.Parse(runIDLayout, record.GetKey().GetRunId())
		if err != nil {
			continue
		}
		if !found || fireTime.After(latest) {
			latest = fireTime
			found = true
		}
	}
	return latest, found, nil
}

// LastFiresFromRuns is a multi-schedule convenience that calls LastFireFromRuns
// for each schedule and returns a snapshot suitable for `Scheduler.Restore`.
func LastFiresFromRuns(
	ctx context.Context,
	store storage.Store,
	namespace string,
	scheduleIDs []string,
	runIDLayout string,
) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(scheduleIDs))
	for _, id := range scheduleIDs {
		t, ok, err := LastFireFromRuns(ctx, store, namespace, id, runIDLayout)
		if err != nil {
			return nil, err
		}
		if ok {
			out[id] = t
		}
	}
	return out, nil
}
