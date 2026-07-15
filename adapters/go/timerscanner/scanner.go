// Package timerscanner finds due timer records for in-flight workflows so a
// serverless or worker process can re-invoke the workflow handler.
//
// Discovery is delegated to Store.DueTimers so a bucket store can use its
// compact due ledger and a remote ConnectStore can perform the same operation
// through RecordStoreService without cross-run list scans in this adapter.
package timerscanner

import (
	"context"
	"fmt"
	"time"

	"github.com/jim-technologies/temporaless/core/go/storage"
)

// DueTimer is a SCHEDULED timer whose fire_at has passed, paired with its
// owning workflow record. The alias preserves the adapter's public API while
// using the core storage result directly.
type DueTimer = storage.DueTimer

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
	return store.DueTimers(ctx, "", now)
}
