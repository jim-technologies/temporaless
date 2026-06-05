// Package janitor sweeps completed workflow runs older than a max-age threshold
// and recursively deletes every record under the run via the Store interface.
//
// It is the simplest workable retention story: completed workflows are kept
// around as long as the operator wants them, and removed when they are no
// longer interesting. Backend-agnostic — works against any Store, including a
// remote ConnectStore client.
package janitor

import (
	"context"
	"fmt"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// Sweep deletes every COMPLETED workflow run whose completed_at is older than
// `maxAge`. Each run's activities, timers, and events are deleted before the
// workflow record itself.
//
// Returns the number of runs deleted. Stops on the first error and reports the
// count of successful deletions before the error.
func Sweep(
	ctx context.Context,
	store storage.Store,
	now time.Time,
	maxAge time.Duration,
) (int, error) {
	if store == nil {
		return 0, fmt.Errorf("store is required")
	}
	if maxAge <= 0 {
		return 0, fmt.Errorf("maxAge must be > 0")
	}

	cutoff := now.Add(-maxAge)
	completed, err := store.ListWorkflows(ctx, "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, record := range completed {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if completedAt := record.GetCompletedAt(); completedAt == nil || completedAt.AsTime().After(cutoff) {
			continue
		}
		key := storage.WorkflowKeyFromProto(record.GetKey())
		if err := deleteRun(ctx, store, key); err != nil {
			return deleted, fmt.Errorf("delete run %s/%s: %w", key.WorkflowID, key.RunID, err)
		}
		deleted++
	}
	return deleted, nil
}

func deleteRun(ctx context.Context, store storage.Store, key storage.WorkflowKey) error {
	activities, err := store.ListActivities(ctx, key)
	if err != nil {
		return err
	}
	for _, record := range activities {
		if _, err := store.DeleteActivity(ctx, storage.ActivityKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	timers, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		return err
	}
	for _, record := range timers {
		if _, err := store.DeleteTimer(ctx, storage.TimerKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	events, err := store.ListEvents(ctx, key)
	if err != nil {
		return err
	}
	for _, record := range events {
		if _, err := store.DeleteEvent(ctx, storage.EventKeyFromProto(record.GetKey())); err != nil {
			return err
		}
	}
	if _, err := store.DeleteWorkflow(ctx, key); err != nil {
		return err
	}
	return nil
}
