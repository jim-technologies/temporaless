package storage

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type OpenDALStore struct {
	operator        *opendal.Operator
	latestPointerMu sync.Mutex
}

func NewOpenDALStore(operator *opendal.Operator) *OpenDALStore {
	return &OpenDALStore{operator: operator}
}

func (store *OpenDALStore) GetActivity(ctx context.Context, key ActivityKey) (*temporalessv1.ActivityRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := key.Path()
	if err != nil {
		return nil, false, err
	}

	exists, err := store.operator.IsExist(path)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	data, err := store.operator.Read(path)
	if err != nil {
		return nil, false, err
	}

	record := &temporalessv1.ActivityRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, false, corruptRecordf("decode activity payload at %s: %v", path, err)
	}
	if err := ValidateActivityRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) GetWorkflow(ctx context.Context, key WorkflowKey) (*temporalessv1.WorkflowRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := key.Path()
	if err != nil {
		return nil, false, err
	}

	exists, err := store.operator.IsExist(path)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	data, err := store.operator.Read(path)
	if err != nil {
		return nil, false, err
	}

	record := &temporalessv1.WorkflowRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, false, corruptRecordf("decode workflow payload at %s: %v", path, err)
	}
	if err := ValidateWorkflowRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) GetTimer(ctx context.Context, key TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	entry, prepared, err := store.getDueEntry(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if prepared {
		if entry.GetRecord().GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
			return nil, false, nil
		}
		return proto.Clone(entry.GetRecord()).(*temporalessv1.TimerRecord), true, nil
	}
	return store.getTimerPoint(ctx, key)
}

func (store *OpenDALStore) getTimerPoint(ctx context.Context, key TimerKey) (*temporalessv1.TimerRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := key.Path()
	if err != nil {
		return nil, false, err
	}

	exists, err := store.operator.IsExist(path)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	data, err := store.operator.Read(path)
	if err != nil {
		return nil, false, err
	}

	record := &temporalessv1.TimerRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, false, corruptRecordf("decode timer payload at %s: %v", path, err)
	}
	if err := ValidateTimerRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := ActivityKeyFromProto(record.GetKey())
	if err := ValidateActivityRecord(record, key); err != nil {
		return err
	}
	path, err := key.Path()
	if err != nil {
		return err
	}
	dir, err := key.DirPath()
	if err != nil {
		return err
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return err
	}

	if err := store.operator.CreateDir(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.operator.Write(path, data)
}

func (store *OpenDALStore) PutWorkflow(ctx context.Context, record *temporalessv1.WorkflowRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := WorkflowKeyFromProto(record.GetKey())
	if err := ValidateWorkflowRecord(record, key); err != nil {
		return err
	}
	pointerEligible := workflowStatusHasLatestPointer(record.GetStatus())
	var recordTime time.Time
	var runOrderTime time.Time
	if pointerEligible {
		var err error
		recordTime, err = workflowLatestPointerRecordTime(record)
		if err != nil {
			return err
		}
		runOrderTime = recordTime
		if record.GetRunOrderTime() != nil {
			runOrderTime = record.GetRunOrderTime().AsTime()
		}
	}
	path, err := key.Path()
	if err != nil {
		return err
	}
	dir, err := key.DirPath()
	if err != nil {
		return err
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return err
	}

	if err := store.operator.CreateDir(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.operator.Write(path, data); err != nil {
		return err
	}
	if !pointerEligible {
		return nil
	}
	return store.putLatestWorkflowRun(ctx, record, recordTime, runOrderTime)
}

func (store *OpenDALStore) GetLatestWorkflowRun(
	ctx context.Context,
	namespace string,
	workflowID string,
) (*temporalessv1.LatestWorkflowRunPointer, bool, error) {
	pointer, found, err := store.getLatestWorkflowRunPointer(ctx, namespace, workflowID)
	if err != nil || !found {
		return pointer, found, err
	}
	referenceKey := WorkflowKeyFromProto(pointer.GetKey())
	reference, referenceFound, err := store.GetWorkflow(ctx, referenceKey)
	if err != nil {
		return nil, false, err
	}
	if !referenceFound {
		// The pointer is derived state and retention may have removed its run.
		// Never return an invented run; a later newer PutWorkflow can replace
		// this stale pointer through the normal writer compare.
		return nil, false, nil
	}
	if err := ValidateLatestWorkflowRunReference(pointer, reference); err != nil {
		if errors.Is(err, ErrStaleLatestPointer) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return pointer, true, nil
}

// getLatestWorkflowRunPointer reads and shape-checks the derived pointer
// without dereferencing its workflow. Writers need this after the
// authoritative record has changed status but before the pointer catches up;
// public reads always call GetLatestWorkflowRun and perform the second point
// GET.
func (store *OpenDALStore) getLatestWorkflowRunPointer(
	ctx context.Context,
	namespace string,
	workflowID string,
) (*temporalessv1.LatestWorkflowRunPointer, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := latestWorkflowRunPointerPath(namespace, workflowID)
	if err != nil {
		return nil, false, err
	}
	exists, err := store.operator.IsExist(path)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	data, err := store.operator.Read(path)
	if err != nil {
		return nil, false, err
	}
	pointer := &temporalessv1.LatestWorkflowRunPointer{}
	if err := proto.Unmarshal(data, pointer); err != nil {
		return nil, false, corruptRecordf("decode latest workflow run pointer at %s: %v", path, err)
	}
	if err := ValidateLatestWorkflowRunPointer(pointer, namespace, workflowID); err != nil {
		return nil, false, err
	}
	return pointer, true, nil
}

func (store *OpenDALStore) putLatestWorkflowRun(
	ctx context.Context,
	record *temporalessv1.WorkflowRecord,
	recordTime time.Time,
	runOrderTime time.Time,
) error {
	store.latestPointerMu.Lock()
	defer store.latestPointerMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	key := WorkflowKeyFromProto(record.GetKey()).withDefaults()
	existing, found, err := store.getLatestWorkflowRunPointer(ctx, key.Namespace, key.WorkflowID)
	if err != nil {
		return err
	}
	if found {
		replace, err := shouldReplaceLatestWorkflowRun(
			existing,
			runOrderTime,
			recordTime,
		)
		if err != nil {
			return err
		}
		if !replace {
			return nil
		}
	}

	pointer := &temporalessv1.LatestWorkflowRunPointer{
		Key:          key.Proto(),
		Status:       record.GetStatus(),
		RecordTime:   timestamppb.New(recordTime),
		UpdatedAt:    timestamppb.Now(),
		RunOrderTime: timestamppb.New(runOrderTime),
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(pointer)
	if err != nil {
		return err
	}
	path, err := latestWorkflowRunPointerPath(key.Namespace, key.WorkflowID)
	if err != nil {
		return err
	}
	dir := path[:strings.LastIndex(path, "/")+1]
	if err := store.operator.CreateDir(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.operator.Write(path, data)
}

func workflowStatusHasLatestPointer(status temporalessv1.WorkflowStatus) bool {
	switch status {
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		return true
	default:
		return false
	}
}

func workflowLatestPointerRecordTime(record *temporalessv1.WorkflowRecord) (time.Time, error) {
	if record == nil {
		return time.Time{}, fmt.Errorf("workflow record is required")
	}
	if (record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED ||
		record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED) && record.GetCompletedAt() != nil {
		if err := record.GetCompletedAt().CheckValid(); err != nil {
			return time.Time{}, fmt.Errorf("invalid workflow completed_at: %w", err)
		}
		return record.GetCompletedAt().AsTime(), nil
	}
	if record.GetCreatedAt() != nil {
		if err := record.GetCreatedAt().CheckValid(); err != nil {
			return time.Time{}, fmt.Errorf("invalid workflow created_at: %w", err)
		}
		return record.GetCreatedAt().AsTime(), nil
	}
	return time.Now().UTC(), nil
}

func shouldReplaceLatestWorkflowRun(
	existing *temporalessv1.LatestWorkflowRunPointer,
	incomingRunOrderTime time.Time,
	incomingRecordTime time.Time,
) (bool, error) {
	if existing == nil || existing.GetRunOrderTime() == nil {
		return false, fmt.Errorf("latest workflow run pointer has no run_order_time")
	}
	if err := existing.GetRunOrderTime().CheckValid(); err != nil {
		return false, fmt.Errorf("latest workflow run pointer has invalid run_order_time: %w", err)
	}
	if existing.GetRecordTime() == nil {
		return false, fmt.Errorf("latest workflow run pointer has no record_time")
	}
	if err := existing.GetRecordTime().CheckValid(); err != nil {
		return false, fmt.Errorf("latest workflow run pointer has invalid record_time: %w", err)
	}
	existingRunOrderTime := existing.GetRunOrderTime().AsTime()
	if !incomingRunOrderTime.Equal(existingRunOrderTime) {
		return incomingRunOrderTime.After(existingRunOrderTime), nil
	}
	return !incomingRecordTime.Before(existing.GetRecordTime().AsTime()), nil
}

func (store *OpenDALStore) GetEvent(ctx context.Context, key EventKey) (*temporalessv1.EventRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := key.Path()
	if err != nil {
		return nil, false, err
	}

	exists, err := store.operator.IsExist(path)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	data, err := store.operator.Read(path)
	if err != nil {
		return nil, false, err
	}

	record := &temporalessv1.EventRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, false, corruptRecordf("decode event payload at %s: %v", path, err)
	}
	if err := ValidateEventRecord(record, key); err != nil {
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := EventKeyFromProto(record.GetKey())
	if err := ValidateEventRecord(record, key); err != nil {
		return err
	}
	path, err := key.Path()
	if err != nil {
		return err
	}
	dir, err := key.DirPath()
	if err != nil {
		return err
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return err
	}

	if err := store.operator.CreateDir(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return store.operator.Write(path, data)
}

func (store *OpenDALStore) DeleteWorkflow(ctx context.Context, key WorkflowKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
	// Latest pointers are derived and intentionally retained. Generic OpenDAL
	// has no generation-aware conditional delete, so deleting one here could
	// race a different store instance publishing a newer run's pointer.
	return deleteIfExists(store.operator, path)
}

func (store *OpenDALStore) ListActivities(
	ctx context.Context,
	key WorkflowKey,
) ([]*temporalessv1.ActivityRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := ActivityKey{
		Namespace:  key.Namespace,
		WorkflowID: key.WorkflowID,
		RunID:      key.RunID,
		ActivityID: "placeholder",
	}.DirPath()
	if err != nil {
		return nil, err
	}
	paths, err := walkOpenDAL(ctx, store.operator, dir)
	if err != nil {
		return nil, err
	}
	var records []*temporalessv1.ActivityRecord
	for _, path := range paths {
		if !strings.HasSuffix(path, ".binpb") {
			continue
		}
		record := &temporalessv1.ActivityRecord{}
		if err := readListedProto(ctx, store.operator, path, record); err != nil {
			return nil, err
		}
		if err := validateListedActivity(path, key, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (store *OpenDALStore) DeleteActivity(ctx context.Context, key ActivityKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
	return deleteIfExists(store.operator, path)
}

func (store *OpenDALStore) ListTimers(
	ctx context.Context,
	key WorkflowKey,
	status temporalessv1.TimerStatus,
) ([]*temporalessv1.TimerRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Read the deterministic due-ledger shadows before canonical points. A
	// valid shadow is the write-ahead copy for exactly one TimerKey, so it can
	// safely cover a corrupt canonical point at that key without parsing
	// identity back out of the point path.
	dueDir, err := dueRunRoot(key)
	if err != nil {
		return nil, err
	}
	duePaths, err := walkOpenDAL(ctx, store.operator, dueDir)
	if err != nil {
		return nil, err
	}
	recordsByID := make(map[string]*temporalessv1.TimerRecord)
	shadowPointPaths := make(map[string]struct{}, len(duePaths))
	for _, duePath := range duePaths {
		entry := &temporalessv1.DueTimerEntry{}
		if err := readListedProto(ctx, store.operator, duePath, entry); err != nil {
			return nil, err
		}
		timerKey := TimerKeyFromProto(entry.GetKey()).withDefaults()
		workflowKey := WorkflowKeyFromProto(entry.GetWorkflowKey()).withDefaults()
		if err := validateDueEntry(dueRoot(key.withDefaults().Namespace), duePath, entry, timerKey, workflowKey); err != nil {
			return nil, corruptRecordf("invalid due entry in timer run listing: %v", err)
		}
		if !sameWorkflowRun(key, workflowKey) {
			return nil, corruptRecordf("due entry payload key does not match listed workflow run at %s", duePath)
		}
		pointPath, err := timerKey.Path()
		if err != nil {
			return nil, err
		}
		shadowPointPaths[pointPath] = struct{}{}
		if entry.GetRecord().GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
			continue
		}
		recordsByID[timerKey.TimerID] = proto.Clone(entry.GetRecord()).(*temporalessv1.TimerRecord)
	}

	dir, err := TimerKey{
		Namespace:  key.Namespace,
		WorkflowID: key.WorkflowID,
		RunID:      key.RunID,
		TimerID:    "placeholder",
	}.DirPath()
	if err != nil {
		return nil, err
	}
	paths, err := walkOpenDAL(ctx, store.operator, dir)
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		if !strings.HasSuffix(path, ".binpb") {
			continue
		}
		record := &temporalessv1.TimerRecord{}
		if err := readListedProto(ctx, store.operator, path, record); err != nil {
			if _, prepared := shadowPointPaths[path]; prepared && errors.Is(err, ErrCorruptRecord) {
				continue
			}
			return nil, err
		}
		if err := validateListedTimer(path, key, record); err != nil {
			if _, prepared := shadowPointPaths[path]; prepared && errors.Is(err, ErrCorruptRecord) {
				continue
			}
			return nil, err
		}
		pointPath, err := TimerKeyFromProto(record.GetKey()).Path()
		if err != nil {
			return nil, err
		}
		if _, prepared := shadowPointPaths[pointPath]; prepared {
			continue
		}
		recordsByID[TimerKeyFromProto(record.GetKey()).TimerID] = record
	}

	records := make([]*temporalessv1.TimerRecord, 0, len(recordsByID))
	for _, record := range recordsByID {
		if status == temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED || record.GetStatus() == status {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(left, right int) bool {
		return TimerKeyFromProto(records[left].GetKey()).TimerID < TimerKeyFromProto(records[right].GetKey()).TimerID
	})
	return records, nil
}

func (store *OpenDALStore) DeleteTimer(ctx context.Context, key TimerKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
	pointExists, err := store.operator.IsExist(path)
	if err != nil {
		return false, err
	}
	entry, ledgerExists, err := store.getDueEntry(ctx, key)
	if err != nil {
		return false, err
	}
	if !pointExists && (!ledgerExists || entry.GetRecord().GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED) {
		return false, nil
	}
	// Publish a durable tombstone before deleting the point record. The
	// deterministic ledger object is deliberately retained so an older
	// ledger-first write can never resurrect an intentionally deleted timer.
	var tombstone *temporalessv1.TimerRecord
	if ledgerExists {
		tombstone = proto.Clone(entry.GetRecord()).(*temporalessv1.TimerRecord)
	} else {
		record, found, pointErr := store.getTimerPoint(ctx, key)
		if pointErr != nil {
			if errors.Is(pointErr, ErrCorruptRecord) {
				// Identity comes from the requested point key, never the damaged
				// payload. Publish a minimal exact-key tombstone before deleting the
				// corrupt bytes so an interrupted delete cannot expose them again.
				tombstone = &temporalessv1.TimerRecord{
					SchemaVersion: TimerRecordSchemaVersion,
					Key:           key.withDefaults().Proto(),
					Status:        temporalessv1.TimerStatus_TIMER_STATUS_CANCELED,
				}
			} else {
				return false, pointErr
			}
		} else if found {
			tombstone = proto.Clone(record).(*temporalessv1.TimerRecord)
		}
	}
	if tombstone != nil {
		tombstone.Status = temporalessv1.TimerStatus_TIMER_STATUS_CANCELED
		tombstone.FiredAt = nil
		if _, err := store.putDueEntry(ctx, tombstone); err != nil {
			return false, err
		}
	}
	if _, err := deleteIfExists(store.operator, path); err != nil {
		return false, err
	}
	return true, nil
}

func (store *OpenDALStore) ListEvents(
	ctx context.Context,
	key WorkflowKey,
) ([]*temporalessv1.EventRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir, err := EventKey{
		Namespace:  key.Namespace,
		WorkflowID: key.WorkflowID,
		RunID:      key.RunID,
		EventID:    "placeholder",
	}.DirPath()
	if err != nil {
		return nil, err
	}
	paths, err := walkOpenDAL(ctx, store.operator, dir)
	if err != nil {
		return nil, err
	}
	var records []*temporalessv1.EventRecord
	for _, path := range paths {
		if !strings.HasSuffix(path, ".binpb") {
			continue
		}
		record := &temporalessv1.EventRecord{}
		if err := readListedProto(ctx, store.operator, path, record); err != nil {
			return nil, err
		}
		if err := validateListedEvent(path, key, record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (store *OpenDALStore) DeleteEvent(ctx context.Context, key EventKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
	return deleteIfExists(store.operator, path)
}

func sameWorkflowRun(left WorkflowKey, right WorkflowKey) bool {
	leftNamespace := left.Namespace
	if leftNamespace == "" {
		leftNamespace = DefaultNamespace
	}
	rightNamespace := right.Namespace
	if rightNamespace == "" {
		rightNamespace = DefaultNamespace
	}
	return leftNamespace == rightNamespace && left.WorkflowID == right.WorkflowID && left.RunID == right.RunID
}

func validateListedActivity(path string, target WorkflowKey, record *temporalessv1.ActivityRecord) error {
	key := ActivityKeyFromProto(record.GetKey())
	if err := ValidateActivityRecord(record, key); err != nil {
		return fmt.Errorf("%w at %s", err, path)
	}
	if !sameWorkflowRun(target, WorkflowKey{Namespace: key.Namespace, WorkflowID: key.WorkflowID, RunID: key.RunID}) {
		return corruptRecordf("activity payload key does not match listed workflow run at %s", path)
	}
	expected, err := key.Path()
	if err != nil {
		return err
	}
	if expected != path {
		return corruptRecordf("activity payload key does not match its storage location at %s", path)
	}
	return nil
}

func validateListedTimer(path string, target WorkflowKey, record *temporalessv1.TimerRecord) error {
	key := TimerKeyFromProto(record.GetKey())
	if err := ValidateTimerRecord(record, key); err != nil {
		return fmt.Errorf("%w at %s", err, path)
	}
	if !sameWorkflowRun(target, WorkflowKey{Namespace: key.Namespace, WorkflowID: key.WorkflowID, RunID: key.RunID}) {
		return corruptRecordf("timer payload key does not match listed workflow run at %s", path)
	}
	expected, err := key.Path()
	if err != nil {
		return err
	}
	if expected != path {
		return corruptRecordf("timer payload key does not match its storage location at %s", path)
	}
	return nil
}

func validateListedEvent(path string, target WorkflowKey, record *temporalessv1.EventRecord) error {
	key := EventKeyFromProto(record.GetKey())
	if err := ValidateEventRecord(record, key); err != nil {
		return fmt.Errorf("%w at %s", err, path)
	}
	if !sameWorkflowRun(target, WorkflowKey{Namespace: key.Namespace, WorkflowID: key.WorkflowID, RunID: key.RunID}) {
		return corruptRecordf("event payload key does not match listed workflow run at %s", path)
	}
	expected, err := key.Path()
	if err != nil {
		return err
	}
	if expected != path {
		return corruptRecordf("event payload key does not match its storage location at %s", path)
	}
	return nil
}

func (store *OpenDALStore) DueTimers(ctx context.Context, namespace string, now time.Time) ([]DueTimer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if now.IsZero() {
		return nil, fmt.Errorf("now is required")
	}
	roots, err := store.dueRoots(ctx, namespace)
	if err != nil {
		return nil, err
	}
	var due []DueTimer
	for _, ledgerRoot := range roots {
		paths, err := dueLedgerPaths(ctx, store.operator, ledgerRoot.path)
		if err != nil {
			return nil, err
		}
		for _, ledgerPath := range paths {
			entry := &temporalessv1.DueTimerEntry{}
			if err := readListedProto(ctx, store.operator, ledgerPath, entry); err != nil {
				// A listing may briefly contain an object that has already been
				// removed. That stale observation is not corruption and carries no
				// payload to quarantine.
				if isOpenDALNotFound(err) {
					continue
				}
				// Quarantine is a best-effort diagnostic copy. The source remains
				// intact because an unconditional delete can race a writer that is
				// publishing the authoritative TimerRecord.
				scanErr := fmt.Errorf("read due timer entry at %s: %w", ledgerPath, err)
				if quarantineErr := quarantineDueEntry(ctx, store.operator, ledgerRoot.quarantinePath, ledgerPath); quarantineErr != nil {
					return nil, errors.Join(
						scanErr,
						fmt.Errorf("quarantine invalid due timer entry at %s: %w", ledgerPath, quarantineErr),
					)
				}
				return nil, scanErr
			}
			timerKey := TimerKeyFromProto(entry.GetKey()).withDefaults()
			workflowKey := WorkflowKeyFromProto(entry.GetWorkflowKey()).withDefaults()
			if err := validateDueEntry(ledgerRoot.path, ledgerPath, entry, timerKey, workflowKey); err != nil {
				scanErr := corruptRecordf("invalid due timer entry at %s: %v", ledgerPath, err)
				if quarantineErr := quarantineDueEntry(ctx, store.operator, ledgerRoot.quarantinePath, ledgerPath); quarantineErr != nil {
					return nil, errors.Join(
						scanErr,
						fmt.Errorf("quarantine invalid due timer entry at %s: %w", ledgerPath, quarantineErr),
					)
				}
				return nil, scanErr
			}
			prepared := entry.GetRecord()
			point, pointFound, pointErr := store.getTimerPoint(ctx, timerKey)
			if pointErr != nil && !errors.Is(pointErr, ErrCorruptRecord) {
				return nil, pointErr
			}
			if prepared.GetStatus() == temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
				// A tombstone is the durable half of DeleteTimer. Complete an
				// interrupted delete before considering any wakes in this scan.
				if pointFound || pointErr != nil {
					pointPath, pathErr := timerKey.Path()
					if pathErr != nil {
						return nil, pathErr
					}
					if _, deleteErr := deleteIfExists(store.operator, pointPath); deleteErr != nil {
						return nil, deleteErr
					}
				}
				continue
			}
			if pointErr != nil || !pointFound || !proto.Equal(point, prepared) {
				// The deterministic ledger is the write-ahead overlay. Repair a
				// missing, stale, or corrupt canonical point, but wait for the next
				// scan before dispatch so every emitted wake has two exact copies.
				if err := store.PutTimer(ctx, proto.Clone(prepared).(*temporalessv1.TimerRecord)); err != nil {
					return nil, err
				}
				continue
			}
			if prepared.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED ||
				entry.GetFireAt().AsTime().After(now) {
				continue
			}

			workflow, workflowFound, err := store.GetWorkflow(ctx, workflowKey)
			if err != nil {
				if errors.Is(err, ErrCorruptRecord) {
					return nil, fmt.Errorf(
						"read parent workflow %q/%q for due timer %q: %w",
						workflowKey.WorkflowID,
						workflowKey.RunID,
						timerKey.TimerID,
						err,
					)
				}
				return nil, err
			}
			if !workflowFound ||
				workflow.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
				continue
			}
			due = append(due, DueTimer{
				Key:      timerKey,
				Record:   proto.Clone(prepared).(*temporalessv1.TimerRecord),
				Workflow: workflow,
			})
		}
	}
	sort.Slice(due, func(left, right int) bool {
		leftFireAt := due[left].Record.GetFireAt().AsTime()
		rightFireAt := due[right].Record.GetFireAt().AsTime()
		if !leftFireAt.Equal(rightFireAt) {
			return leftFireAt.Before(rightFireAt)
		}
		if due[left].Key.Namespace != due[right].Key.Namespace {
			return due[left].Key.Namespace < due[right].Key.Namespace
		}
		if due[left].Key.WorkflowID != due[right].Key.WorkflowID {
			return due[left].Key.WorkflowID < due[right].Key.WorkflowID
		}
		if due[left].Key.RunID != due[right].Key.RunID {
			return due[left].Key.RunID < due[right].Key.RunID
		}
		return due[left].Key.TimerID < due[right].Key.TimerID
	})
	return due, nil
}

func dueRoot(namespace string) string {
	return fmt.Sprintf("%s/%s/_due/", StorageRootPrefix, namespace)
}

func dueInvalidRoot(namespace string) string {
	return fmt.Sprintf("%s/%s/_due_invalid/", StorageRootPrefix, namespace)
}

func dueEntryPath(key TimerKey) (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s%s/%s/%s.binpb",
		dueRoot(key.Namespace),
		key.WorkflowID,
		key.RunID,
		key.TimerID,
	), nil
}

func dueRunRoot(key WorkflowKey) (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s/%s/", dueRoot(key.Namespace), key.WorkflowID, key.RunID), nil
}

type dueLedgerRoot struct {
	path           string
	quarantinePath string
}

func (store *OpenDALStore) dueRoots(ctx context.Context, namespace string) ([]dueLedgerRoot, error) {
	if namespace != "" {
		probe := WorkflowKey{Namespace: namespace, WorkflowID: "placeholder", RunID: "placeholder"}
		if err := probe.Validate(); err != nil {
			return nil, err
		}
		return []dueLedgerRoot{{
			path:           dueRoot(namespace),
			quarantinePath: dueInvalidRoot(namespace),
		}}, nil
	}

	root := StorageRootPrefix + "/"
	lister, err := store.operator.List(root)
	if err != nil {
		if isOpenDALNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var roots []dueLedgerRoot
	for lister.Next() {
		if err := ctx.Err(); err != nil {
			_ = lister.Close()
			return nil, err
		}
		path := lister.Entry().Path()
		if path == root || !strings.HasSuffix(path, "/") {
			continue
		}
		namespaceRoot := strings.TrimSuffix(path, "/")
		roots = append(roots, dueLedgerRoot{
			path:           namespaceRoot + "/_due/",
			quarantinePath: namespaceRoot + "/_due_invalid/",
		})
	}
	closeErr := lister.Close()
	if listerErr := lister.Error(); listerErr != nil {
		return nil, listerErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	sort.Slice(roots, func(left, right int) bool { return roots[left].path < roots[right].path })
	return roots, nil
}

func dueLedgerPaths(
	ctx context.Context,
	operator *opendal.Operator,
	root string,
) ([]string, error) {
	return walkOpenDAL(ctx, operator, root)
}

func validateDueEntry(
	root string,
	ledgerPath string,
	entry *temporalessv1.DueTimerEntry,
	timerKey TimerKey,
	workflowKey WorkflowKey,
) error {
	if entry.GetRecord() == nil {
		return fmt.Errorf("due timer entry at %s has no prepared record", ledgerPath)
	}
	if err := timerKey.Validate(); err != nil {
		return fmt.Errorf("due timer entry at %s has invalid timer key: %w", ledgerPath, err)
	}
	if err := workflowKey.Validate(); err != nil {
		return fmt.Errorf("due timer entry at %s has invalid workflow key: %w", ledgerPath, err)
	}
	if !sameWorkflowRun(
		workflowKey,
		WorkflowKey{Namespace: timerKey.Namespace, WorkflowID: timerKey.WorkflowID, RunID: timerKey.RunID},
	) {
		return fmt.Errorf("due timer entry at %s crosses workflow runs", ledgerPath)
	}
	if dueRoot(timerKey.Namespace) != root {
		return fmt.Errorf("due timer entry at %s is stored under the wrong namespace", ledgerPath)
	}
	if err := ValidateTimerRecord(entry.GetRecord(), timerKey); err != nil {
		return fmt.Errorf("due timer entry at %s has invalid prepared record: %w", ledgerPath, err)
	}
	switch entry.GetRecord().GetStatus() {
	case temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED:
		if entry.GetRecord().GetFireAt() == nil {
			return fmt.Errorf("scheduled due timer entry at %s has no prepared fire_at", ledgerPath)
		}
	case temporalessv1.TimerStatus_TIMER_STATUS_FIRED,
		temporalessv1.TimerStatus_TIMER_STATUS_CANCELED:
	default:
		return fmt.Errorf("due timer entry at %s has invalid prepared status", ledgerPath)
	}
	preparedFireAt := entry.GetRecord().GetFireAt()
	entryFireAt := entry.GetFireAt()
	if (preparedFireAt == nil) != (entryFireAt == nil) {
		return fmt.Errorf("due timer entry at %s has mismatched prepared fire_at", ledgerPath)
	}
	if preparedFireAt != nil {
		if err := preparedFireAt.CheckValid(); err != nil {
			return fmt.Errorf("due timer entry at %s has invalid prepared fire_at: %w", ledgerPath, err)
		}
		if err := entryFireAt.CheckValid(); err != nil {
			return fmt.Errorf("due timer entry at %s has invalid fire_at: %w", ledgerPath, err)
		}
		if !preparedFireAt.AsTime().Equal(entryFireAt.AsTime()) {
			return fmt.Errorf("due timer entry at %s has mismatched prepared fire_at", ledgerPath)
		}
	}
	expected, err := dueEntryPath(timerKey)
	if err != nil {
		return err
	}
	if expected != ledgerPath {
		return fmt.Errorf("due timer entry payload does not match its storage location at %s", ledgerPath)
	}
	return nil
}

func quarantineDueEntry(
	ctx context.Context,
	operator *opendal.Operator,
	quarantineRoot string,
	ledgerPath string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := operator.Read(ledgerPath)
	if err != nil {
		if isOpenDALNotFound(err) {
			return nil
		}
		return err
	}
	digest := sha256.Sum256([]byte(ledgerPath))
	target := fmt.Sprintf("%s%x.binpb", quarantineRoot, digest)
	dir := target[:strings.LastIndex(target, "/")+1]
	if err := operator.CreateDir(dir); err != nil {
		return err
	}
	if err := operator.Write(target, data); err != nil {
		return err
	}
	return nil
}

func deleteIfExists(operator *opendal.Operator, path string) (bool, error) {
	exists, err := operator.IsExist(path)
	if err != nil {
		return false, err
	}
	if !exists {
		return false, nil
	}
	if err := operator.Delete(path); err != nil {
		if isOpenDALNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func walkOpenDAL(ctx context.Context, operator *opendal.Operator, root string) ([]string, error) {
	var files []string
	queue := []string{root}
	for len(queue) > 0 {
		if err := ctx.Err(); err != nil {
			return files, err
		}
		current := queue[0]
		queue = queue[1:]
		lister, err := operator.List(current)
		if err != nil {
			if isOpenDALNotFound(err) {
				continue
			}
			return nil, err
		}
		for lister.Next() {
			entry := lister.Entry()
			path := entry.Path()
			if path == current {
				continue
			}
			if strings.HasSuffix(path, "/") {
				queue = append(queue, path)
			} else if strings.HasSuffix(path, ".binpb") {
				files = append(files, path)
			}
		}
		closeErr := lister.Close()
		if listerErr := lister.Error(); listerErr != nil {
			return nil, listerErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	sort.Strings(files)
	return files, nil
}

func readListedProto(ctx context.Context, operator *opendal.Operator, path string, message proto.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := operator.Read(path)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return corruptRecordf("decode listed protobuf %s: %v", path, err)
	}
	return nil
}

func isOpenDALNotFound(err error) bool {
	var oe *opendal.Error
	if errors.As(err, &oe) {
		return oe.Code() == opendal.CodeNotFound
	}
	return false
}

func (store *OpenDALStore) PutTimer(ctx context.Context, record *temporalessv1.TimerRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := TimerKeyFromProto(record.GetKey()).withDefaults()
	if err := ValidateTimerRecord(record, key); err != nil {
		return err
	}
	path, err := key.Path()
	if err != nil {
		return err
	}
	dir, err := key.DirPath()
	if err != nil {
		return err
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(record)
	if err != nil {
		return err
	}

	if err := store.operator.CreateDir(dir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Publish the full prepared timer at its deterministic discovery point
	// before the canonical run record. If the process dies between writes,
	// GetTimer/ListTimers replay this exact protobuf (including the original
	// deadline) instead of restarting the duration from replay time.
	if _, err := store.putDueEntry(ctx, record); err != nil {
		return err
	}
	return store.operator.Write(path, data)
}

func (store *OpenDALStore) putDueEntry(
	ctx context.Context,
	record *temporalessv1.TimerRecord,
) (string, error) {
	key := TimerKeyFromProto(record.GetKey()).withDefaults()
	if err := ValidateTimerRecord(record, key); err != nil {
		return "", err
	}
	switch record.GetStatus() {
	case temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED:
		if record.GetFireAt() == nil {
			return "", fmt.Errorf("scheduled due timer fire_at is required")
		}
	case temporalessv1.TimerStatus_TIMER_STATUS_FIRED,
		temporalessv1.TimerStatus_TIMER_STATUS_CANCELED:
	default:
		return "", fmt.Errorf("due timer status must be SCHEDULED, FIRED, or CANCELED")
	}
	if record.GetFireAt() != nil {
		if err := record.GetFireAt().CheckValid(); err != nil {
			return "", fmt.Errorf("invalid due timer fire_at: %w", err)
		}
	}
	path, err := dueEntryPath(key)
	if err != nil {
		return "", err
	}
	entry := &temporalessv1.DueTimerEntry{
		Key: key.Proto(),
		WorkflowKey: (&WorkflowKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
		}).Proto(),
		Record: proto.Clone(record).(*temporalessv1.TimerRecord),
	}
	if record.GetFireAt() != nil {
		entry.FireAt = proto.Clone(record.GetFireAt()).(*timestamppb.Timestamp)
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
	if err != nil {
		return "", err
	}
	dir := path[:strings.LastIndex(path, "/")+1]
	if err := store.operator.CreateDir(dir); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := store.operator.Write(path, data); err != nil {
		return "", err
	}
	return path, nil
}

func (store *OpenDALStore) getDueEntry(
	ctx context.Context,
	key TimerKey,
) (*temporalessv1.DueTimerEntry, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	path, err := dueEntryPath(key)
	if err != nil {
		return nil, false, err
	}
	exists, err := store.operator.IsExist(path)
	if err != nil || !exists {
		return nil, false, err
	}
	entry := &temporalessv1.DueTimerEntry{}
	if err := readListedProto(ctx, store.operator, path, entry); err != nil {
		return nil, false, err
	}
	timerKey := TimerKeyFromProto(entry.GetKey()).withDefaults()
	workflowKey := WorkflowKeyFromProto(entry.GetWorkflowKey()).withDefaults()
	if err := validateDueEntry(dueRoot(key.withDefaults().Namespace), path, entry, timerKey, workflowKey); err != nil {
		return nil, false, corruptRecordf("invalid prepared due timer: %v", err)
	}
	if timerKey != key.withDefaults() {
		return nil, false, corruptRecordf("prepared due timer key does not match requested key")
	}
	return entry, true, nil
}
