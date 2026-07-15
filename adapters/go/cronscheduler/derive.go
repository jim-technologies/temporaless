package cronscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/jim-technologies/temporaless/core/go/storage"
)

// LastFireFromRuns point-reads the latest-run pointer for scheduleID and
// returns its protobuf run_order_time.
//
// Schedule dispatchers set WorkflowOptions.run_order_time to the fire time.
// The pointer persists that timestamp, so SDKs never parse opaque caller-owned
// run IDs or depend on language-specific date formats.
//
// Returns the zero time and ok=false when no valid referenced run exists yet.
func LastFireFromRuns(
	ctx context.Context,
	store storage.Store,
	namespace string,
	scheduleID string,
) (time.Time, bool, error) {
	if store == nil {
		return time.Time{}, false, fmt.Errorf("store is required")
	}
	if scheduleID == "" {
		return time.Time{}, false, fmt.Errorf("scheduleID is required")
	}
	pointer, found, err := store.GetLatestWorkflowRun(ctx, namespace, scheduleID)
	if err != nil {
		return time.Time{}, false, err
	}
	if !found {
		return time.Time{}, false, nil
	}
	if pointer.GetRunOrderTime() == nil {
		return time.Time{}, false, nil
	}
	if err := pointer.GetRunOrderTime().CheckValid(); err != nil {
		return time.Time{}, false, fmt.Errorf("latest run_order_time: %w", err)
	}
	return pointer.GetRunOrderTime().AsTime(), true, nil
}

// LastFiresFromRuns is a multi-schedule convenience that calls LastFireFromRuns
// for each schedule and returns a snapshot suitable for `Scheduler.Restore`.
func LastFiresFromRuns(
	ctx context.Context,
	store storage.Store,
	namespace string,
	scheduleIDs []string,
) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(scheduleIDs))
	for _, id := range scheduleIDs {
		t, ok, err := LastFireFromRuns(ctx, store, namespace, id)
		if err != nil {
			return nil, err
		}
		if ok {
			out[id] = t
		}
	}
	return out, nil
}
