// Package timerscanner finds due timer records for in-flight workflows so a
// serverless or worker process can re-invoke the workflow handler.
//
// It composes Store.ListWorkflows(IN_PROGRESS) with Store.ListTimers(SCHEDULED)
// per run, then filters by fire_at <= now. Backend-agnostic — works against
// any Store, including a remote ConnectStore.
package timerscanner

import (
	"context"
	"fmt"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

// DueTimer is a SCHEDULED timer whose fire_at has passed, paired with the
// workflow record that owns it so callers have enough context to re-invoke.
type DueTimer struct {
	Key      storage.TimerKey
	Record   *temporalessv1.TimerRecord
	Workflow *temporalessv1.WorkflowRecord
}

// DueTimers returns timers belonging to IN_PROGRESS workflows whose fire_at
// has passed.
//
// Stale timers under COMPLETED or FAILED workflows are intentionally skipped:
// the workflow has already moved past them.
func DueTimers(
	ctx context.Context,
	store storage.Store,
	now time.Time,
) ([]DueTimer, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	inFlight, err := store.ListWorkflows(ctx, "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS)
	if err != nil {
		return nil, err
	}

	var due []DueTimer
	for _, workflow := range inFlight {
		if err := ctx.Err(); err != nil {
			return due, err
		}
		key := storage.WorkflowKeyFromProto(workflow.GetKey())
		timers, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
		if err != nil {
			return nil, err
		}
		for _, timer := range timers {
			if fireAt := timer.GetFireAt(); fireAt != nil && fireAt.AsTime().After(now) {
				continue
			}
			due = append(due, DueTimer{
				Key:      storage.TimerKeyFromProto(timer.GetKey()),
				Record:   timer,
				Workflow: workflow,
			})
		}
	}
	return due, nil
}
