package storage

import (
	"fmt"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

// ValidateWorkflowRecord validates a point-read workflow payload against the
// key the caller requested. It is exported so transport-backed Store
// implementations enforce the same trust boundary as OpenDALStore.
func ValidateWorkflowRecord(record *temporalessv1.WorkflowRecord, requested WorkflowKey) error {
	if record == nil {
		return corruptRecordf("workflow payload is required")
	}
	if record.GetSchemaVersion() != WorkflowRecordSchemaVersion {
		return corruptRecordf(
			"workflow payload has schema_version %s, want %s",
			record.GetSchemaVersion(),
			WorkflowRecordSchemaVersion,
		)
	}
	actual := WorkflowKeyFromProto(record.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("workflow payload has invalid key: %v", err)
	}
	if actual != requested.withDefaults() {
		return corruptRecordf("workflow payload key does not match requested location")
	}
	if record.GetRunOrderTime() != nil {
		if err := record.GetRunOrderTime().CheckValid(); err != nil {
			return corruptRecordf("workflow payload has invalid run_order_time: %v", err)
		}
	}
	return nil
}

// ValidateActivityRecord validates a point-read activity payload against the
// key the caller requested.
func ValidateActivityRecord(record *temporalessv1.ActivityRecord, requested ActivityKey) error {
	if record == nil {
		return corruptRecordf("activity payload is required")
	}
	if record.GetSchemaVersion() != ActivityRecordSchemaVersion {
		return corruptRecordf(
			"activity payload has schema_version %s, want %s",
			record.GetSchemaVersion(),
			ActivityRecordSchemaVersion,
		)
	}
	actual := ActivityKeyFromProto(record.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("activity payload has invalid key: %v", err)
	}
	if actual != requested.withDefaults() {
		return corruptRecordf("activity payload key does not match requested location")
	}
	return nil
}

// ValidateTimerRecord validates a point-read timer payload against the key the
// caller requested.
func ValidateTimerRecord(record *temporalessv1.TimerRecord, requested TimerKey) error {
	if record == nil {
		return corruptRecordf("timer payload is required")
	}
	if record.GetSchemaVersion() != TimerRecordSchemaVersion {
		return corruptRecordf(
			"timer payload has schema_version %s, want %s",
			record.GetSchemaVersion(),
			TimerRecordSchemaVersion,
		)
	}
	actual := TimerKeyFromProto(record.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("timer payload has invalid key: %v", err)
	}
	if actual != requested.withDefaults() {
		return corruptRecordf("timer payload key does not match requested location")
	}
	return nil
}

// ValidateEventRecord validates a point-read event payload against the key the
// caller requested.
func ValidateEventRecord(record *temporalessv1.EventRecord, requested EventKey) error {
	if record == nil {
		return corruptRecordf("event payload is required")
	}
	if record.GetSchemaVersion() != EventRecordSchemaVersion {
		return corruptRecordf(
			"event payload has schema_version %s, want %s",
			record.GetSchemaVersion(),
			EventRecordSchemaVersion,
		)
	}
	actual := EventKeyFromProto(record.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("event payload has invalid key: %v", err)
	}
	if actual != requested.withDefaults() {
		return corruptRecordf("event payload key does not match requested location")
	}
	return nil
}

// ValidateEventDeliveryRecord applies the stricter application-delivery
// contract. Low-level PutEvent intentionally remains permissive for operators
// and migrations, but DeliverEvent always requires a payload and timestamp.
func ValidateEventDeliveryRecord(
	record *temporalessv1.EventRecord,
	requested EventKey,
) error {
	if err := ValidateEventRecord(record, requested); err != nil {
		return err
	}
	if record.GetPayload() == nil {
		return corruptRecordf("event delivery payload is required")
	}
	if record.GetReceivedAt() == nil {
		return corruptRecordf("event delivery received_at is required")
	}
	if err := record.GetReceivedAt().CheckValid(); err != nil {
		return corruptRecordf("event delivery has invalid received_at: %v", err)
	}
	return nil
}

// ValidateClaimRecord validates a point-read claim payload against the key the
// caller requested.
func ValidateClaimRecord(record *temporalessv1.ClaimRecord, requested ClaimKey) error {
	if record == nil {
		return corruptRecordf("claim payload is required")
	}
	if record.GetSchemaVersion() != ClaimRecordSchemaVersion {
		return corruptRecordf(
			"claim payload has schema_version %s, want %s",
			record.GetSchemaVersion(),
			ClaimRecordSchemaVersion,
		)
	}
	actual := ClaimKeyFromProto(record.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("claim payload has invalid key: %v", err)
	}
	if actual != requested.withDefaults() {
		return corruptRecordf("claim payload key does not match requested location")
	}
	return nil
}

// ValidateLatestWorkflowRunPointer validates the derived pointer payload
// against the workflow identity whose point path was requested. Existence of
// the referenced run is validated separately because it requires one more
// authoritative point read.
func ValidateLatestWorkflowRunPointer(
	pointer *temporalessv1.LatestWorkflowRunPointer,
	namespace string,
	workflowID string,
) error {
	if pointer == nil {
		return corruptRecordf("latest workflow run pointer is required")
	}
	requested := WorkflowKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      "placeholder",
	}.withDefaults()
	if err := requested.Validate(); err != nil {
		return err
	}
	actual := WorkflowKeyFromProto(pointer.GetKey()).withDefaults()
	if err := actual.Validate(); err != nil {
		return corruptRecordf("latest workflow run pointer has invalid key: %v", err)
	}
	if actual.Namespace != requested.Namespace || actual.WorkflowID != requested.WorkflowID {
		return corruptRecordf("latest workflow run pointer key does not match requested location")
	}
	if !workflowStatusHasLatestPointer(pointer.GetStatus()) {
		return corruptRecordf(
			"latest workflow run pointer has invalid status %s",
			pointer.GetStatus(),
		)
	}
	if pointer.GetRecordTime() == nil {
		return corruptRecordf("latest workflow run pointer has no record_time")
	}
	if err := pointer.GetRecordTime().CheckValid(); err != nil {
		return corruptRecordf("latest workflow run pointer has invalid record_time: %v", err)
	}
	if pointer.GetUpdatedAt() == nil {
		return corruptRecordf("latest workflow run pointer has no updated_at")
	}
	if err := pointer.GetUpdatedAt().CheckValid(); err != nil {
		return corruptRecordf("latest workflow run pointer has invalid updated_at: %v", err)
	}
	if pointer.GetRunOrderTime() == nil {
		return corruptRecordf("latest workflow run pointer has no run_order_time")
	}
	if err := pointer.GetRunOrderTime().CheckValid(); err != nil {
		return corruptRecordf("latest workflow run pointer has invalid run_order_time: %v", err)
	}
	return nil
}

// ValidateLatestWorkflowRunReference proves that a validated latest pointer
// names the authoritative WorkflowRecord returned by its deterministic point
// key. Identity/schema failures are corruption. Metadata differences are
// classified as ErrStaleLatestPointer because readers can legitimately land
// between the authoritative workflow write and its derived pointer update.
func ValidateLatestWorkflowRunReference(
	pointer *temporalessv1.LatestWorkflowRunPointer,
	record *temporalessv1.WorkflowRecord,
) error {
	if pointer == nil {
		return corruptRecordf("latest workflow run pointer is required")
	}
	if err := ValidateWorkflowRecord(record, WorkflowKeyFromProto(pointer.GetKey())); err != nil {
		return err
	}
	if pointer.GetStatus() != record.GetStatus() {
		return staleLatestPointerf("status does not match its referenced workflow")
	}
	expectedRecordTime, present, err := persistedWorkflowRecordTime(record)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}
	if !pointer.GetRecordTime().AsTime().Equal(expectedRecordTime) {
		return staleLatestPointerf("record_time does not match its referenced workflow")
	}
	expectedRunOrderTime := expectedRecordTime
	if record.GetRunOrderTime() != nil {
		if err := record.GetRunOrderTime().CheckValid(); err != nil {
			return corruptRecordf("referenced workflow has invalid run_order_time: %v", err)
		}
		expectedRunOrderTime = record.GetRunOrderTime().AsTime()
	}
	if !pointer.GetRunOrderTime().AsTime().Equal(expectedRunOrderTime) {
		return staleLatestPointerf("run_order_time does not match its referenced workflow")
	}
	return nil
}

// ValidateDueTimer validates a timer/workflow pair returned across a Store or
// transport boundary. It prevents a derived response from redirecting a
// dispatcher to another run or returning future/non-authoritative work.
func ValidateDueTimer(due DueTimer, namespace string, now time.Time) error {
	if err := ValidateTimerRecord(due.Record, due.Key); err != nil {
		return err
	}
	workflowKey := WorkflowKey{
		Namespace:  due.Key.Namespace,
		WorkflowID: due.Key.WorkflowID,
		RunID:      due.Key.RunID,
	}
	if err := ValidateWorkflowRecord(due.Workflow, workflowKey); err != nil {
		return err
	}
	if namespace != "" && due.Key.withDefaults().Namespace != namespace {
		return corruptRecordf("due timer payload crosses the requested namespace")
	}
	if due.Record.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
		return corruptRecordf("due timer payload is not scheduled")
	}
	if due.Workflow.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		return corruptRecordf("due timer workflow is not in progress")
	}
	if due.Record.GetFireAt() == nil {
		return corruptRecordf("due timer payload has no fire_at")
	}
	if err := due.Record.GetFireAt().CheckValid(); err != nil {
		return corruptRecordf("due timer payload has invalid fire_at: %v", err)
	}
	if due.Record.GetFireAt().AsTime().After(now) {
		return corruptRecordf("due timer payload fire_at is later than the requested time")
	}
	return nil
}

func persistedWorkflowRecordTime(record *temporalessv1.WorkflowRecord) (time.Time, bool, error) {
	if record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED ||
		record.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		if record.GetCompletedAt() != nil {
			if err := record.GetCompletedAt().CheckValid(); err != nil {
				return time.Time{}, false, corruptRecordf("referenced workflow has invalid completed_at: %v", err)
			}
			return record.GetCompletedAt().AsTime(), true, nil
		}
	}
	if record.GetCreatedAt() != nil {
		if err := record.GetCreatedAt().CheckValid(); err != nil {
			return time.Time{}, false, corruptRecordf("referenced workflow has invalid created_at: %v", err)
		}
		return record.GetCreatedAt().AsTime(), true, nil
	}
	return time.Time{}, false, nil
}

func corruptRecordf(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptRecord, fmt.Sprintf(format, arguments...))
}

func staleLatestPointerf(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", ErrStaleLatestPointer, fmt.Sprintf(format, arguments...))
}
