package inspection

import (
	"context"
	"errors"
	"fmt"
	"strings"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

// ListScheduledWakes implements RunInspectionService. It walks the
// namespace's due ledger by name and reads at most Limits.ReadBudget entries
// per request. Each SCHEDULED entry is compared with its canonical timer
// record and its parent workflow; disagreement is reported, never repaired.
// This deliberately does not call Store.DueTimers, which repairs the ledger.
func (service *Service) ListScheduledWakes(
	ctx context.Context,
	request *inspectionv1.ListScheduledWakesRequest,
) (*inspectionv1.ListScheduledWakesResponse, error) {
	store, _, err := service.namespaceScope(ctx, request.GetStore(), request.GetNamespace())
	if err != nil {
		return nil, err
	}
	namespace := request.GetNamespace()
	binding := fmt.Sprintf("wakes\x1f%s\x1f%s\x1f%t", store.ID, namespace, request.GetOverdueOnly())
	cursor, err := service.openCursor(ctx, binding, request.GetPageToken())
	if err != nil {
		return nil, err
	}

	paths, err := walkFiles(ctx, store.Bucket, dueDir(namespace), service.limits.MaxListedObjects)
	if err != nil {
		return nil, service.storeError(err)
	}
	quarantined, err := store.Bucket.List(ctx, dueInvalidDir(namespace))
	if err != nil {
		return nil, service.storeError(err)
	}
	if len(quarantined) > service.limits.MaxListedObjects {
		return nil, service.storeError(fmt.Errorf("%w: %d quarantined ledger entries in namespace %q", errTooManyObjects, len(quarantined), namespace))
	}
	response := &inspectionv1.ListScheduledWakesResponse{}
	for _, path := range quarantined {
		if strings.HasSuffix(path, ".binpb") {
			response.QuarantinedEntries++
		}
	}

	candidates := paths
	if cursor != "" {
		candidates = candidates[:0:0]
		for _, path := range paths {
			if path > cursor {
				candidates = append(candidates, path)
			}
		}
	}
	pageSize := service.pageSize(request.GetPageSize())
	budget := service.limits.ReadBudget
	now := service.now()
	overdueBefore := now.Add(-overdueGrace(store))
	next := 0
	lastProcessed := ""
	for next < len(candidates) && len(response.Wakes) < pageSize && int(response.Scanned) < budget {
		batch := candidates[next:min(next+service.limits.Concurrency, len(candidates), next+budget-int(response.Scanned))]
		wakes := make([]*inspectionv1.ScheduledWake, len(batch))
		invalid := make([]bool, len(batch))
		if err := forEach(ctx, service.limits.Concurrency, batch, func(ctx context.Context, index int, path string) error {
			wake, valid, err := service.scheduledWake(ctx, store, namespace, path)
			wakes[index] = wake
			invalid[index] = !valid
			return err
		}); err != nil {
			return nil, service.storeError(err)
		}
		for index, wake := range wakes {
			response.Scanned++
			lastProcessed = batch[index]
			next++
			if invalid[index] {
				response.InvalidEntries++
				continue
			}
			if wake == nil {
				continue
			}
			wake.Overdue = wake.GetTimer().GetFireAt().AsTime().Before(overdueBefore)
			if request.GetOverdueOnly() && !wake.GetOverdue() {
				continue
			}
			response.Wakes = append(response.Wakes, wake)
			if len(response.Wakes) == pageSize {
				break
			}
		}
	}
	if next < len(candidates) {
		if response.NextPageToken, err = service.sealCursor(ctx, binding, lastProcessed); err != nil {
			return nil, err
		}
	}
	return response, nil
}

// scheduledWake reads one ledger entry. It returns (nil, true, nil) for a
// valid entry that is not SCHEDULED or has vanished, and (nil, false, nil)
// for one a timer scanner would quarantine.
func (service *Service) scheduledWake(
	ctx context.Context,
	store *Store,
	namespace string,
	path string,
) (*inspectionv1.ScheduledWake, bool, error) {
	data, found, err := store.Bucket.Read(ctx, path)
	if err != nil || !found {
		return nil, true, err
	}
	entry := &temporalessv1.DueTimerEntry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		return nil, false, nil
	}
	timerKey := storage.TimerKeyFromProto(entry.GetKey())
	workflowKey := storage.WorkflowKeyFromProto(entry.GetWorkflowKey())
	record := entry.GetRecord()
	if record == nil || timerKey.Validate() != nil || workflowKey.Validate() != nil ||
		timerKey.Namespace != namespace || duePath(timerKey) != path ||
		workflowKey != (storage.WorkflowKey{Namespace: timerKey.Namespace, WorkflowID: timerKey.WorkflowID, RunID: timerKey.RunID}) ||
		storage.ValidateTimerRecord(record, timerKey) != nil {
		return nil, false, nil
	}
	if record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		return nil, true, nil
	}
	if record.GetFireAt() == nil || entry.GetFireAt() == nil || !record.GetFireAt().AsTime().Equal(entry.GetFireAt().AsTime()) {
		return nil, false, nil
	}

	wake := &inspectionv1.ScheduledWake{Timer: record, WorkflowKey: entry.GetWorkflowKey()}
	pointPath, err := timerKey.Path()
	if err != nil {
		return nil, false, nil
	}
	pointData, pointFound, err := store.Bucket.Read(ctx, pointPath)
	if err != nil {
		return nil, true, err
	}
	point := &temporalessv1.TimerRecord{}
	switch {
	case !pointFound:
		wake.LedgerState = inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_MISSING
	case proto.Unmarshal(pointData, point) != nil || storage.ValidateTimerRecord(point, timerKey) != nil:
		wake.LedgerState = inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_UNREADABLE
	case !proto.Equal(point, record):
		wake.LedgerState = inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_DIFFERS
	default:
		wake.LedgerState = inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_AGREES
	}

	workflow, workflowFound, err := store.Records.GetWorkflow(ctx, workflowKey)
	switch {
	case errors.Is(err, storage.ErrCorruptRecord):
		service.logger.Warn("inspection: corrupt parent workflow for wake", "store", store.ID, "path", path, "error", err)
	case err != nil:
		return nil, true, err
	case workflowFound:
		wake.WorkflowStatus = workflow.GetStatus()
	}
	return wake, true, nil
}
