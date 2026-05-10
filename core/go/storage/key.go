package storage

import (
	"fmt"

	"buf.build/go/protovalidate"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

const (
	DefaultNamespace            = "default"
	ActivityRecordSchemaVersion = temporalessv1.RecordSchemaVersion_RECORD_SCHEMA_VERSION_ACTIVITY
	ClaimRecordSchemaVersion    = temporalessv1.RecordSchemaVersion_RECORD_SCHEMA_VERSION_CLAIM
	EventRecordSchemaVersion    = temporalessv1.RecordSchemaVersion_RECORD_SCHEMA_VERSION_EVENT
	TimerRecordSchemaVersion    = temporalessv1.RecordSchemaVersion_RECORD_SCHEMA_VERSION_TIMER
	WorkflowRecordSchemaVersion = temporalessv1.RecordSchemaVersion_RECORD_SCHEMA_VERSION_WORKFLOW
	SleepTimerKind              = temporalessv1.TimerKind_TIMER_KIND_SLEEP
)

type WorkflowKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
}

type ActivityKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
	ActivityID string
}

type TimerKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
	TimerID    string
}

type ClaimKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
	ClaimID    string
}

type EventKey struct {
	Namespace  string
	WorkflowID string
	RunID      string
	EventID    string
}

func NewWorkflowKey(workflowID string, runID string) WorkflowKey {
	return WorkflowKey{
		Namespace:  DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
}

func NewActivityKey(workflowID string, runID string, activityID string) ActivityKey {
	return ActivityKey{
		Namespace:  DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
		ActivityID: activityID,
	}
}

func NewTimerKey(workflowID string, runID string, timerID string) TimerKey {
	return TimerKey{
		Namespace:  DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
		TimerID:    timerID,
	}
}

func NewClaimKey(workflowID string, runID string, claimID string) ClaimKey {
	return ClaimKey{
		Namespace:  DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
		ClaimID:    claimID,
	}
}

func NewEventKey(workflowID string, runID string, eventID string) EventKey {
	return EventKey{
		Namespace:  DefaultNamespace,
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    eventID,
	}
}

func WorkflowKeyFromProto(key *temporalessv1.WorkflowKey) WorkflowKey {
	if key == nil {
		return WorkflowKey{}
	}
	return WorkflowKey{
		Namespace:  key.GetNamespace(),
		WorkflowID: key.GetWorkflowId(),
		RunID:      key.GetRunId(),
	}
}

func ActivityKeyFromProto(key *temporalessv1.ActivityKey) ActivityKey {
	if key == nil {
		return ActivityKey{}
	}
	return ActivityKey{
		Namespace:  key.GetNamespace(),
		WorkflowID: key.GetWorkflowId(),
		RunID:      key.GetRunId(),
		ActivityID: key.GetActivityId(),
	}
}

func TimerKeyFromProto(key *temporalessv1.TimerKey) TimerKey {
	if key == nil {
		return TimerKey{}
	}
	return TimerKey{
		Namespace:  key.GetNamespace(),
		WorkflowID: key.GetWorkflowId(),
		RunID:      key.GetRunId(),
		TimerID:    key.GetTimerId(),
	}
}

func ClaimKeyFromProto(key *temporalessv1.ClaimKey) ClaimKey {
	if key == nil {
		return ClaimKey{}
	}
	return ClaimKey{
		Namespace:  key.GetNamespace(),
		WorkflowID: key.GetWorkflowId(),
		RunID:      key.GetRunId(),
		ClaimID:    key.GetClaimId(),
	}
}

func EventKeyFromProto(key *temporalessv1.EventKey) EventKey {
	if key == nil {
		return EventKey{}
	}
	return EventKey{
		Namespace:  key.GetNamespace(),
		WorkflowID: key.GetWorkflowId(),
		RunID:      key.GetRunId(),
		EventID:    key.GetEventId(),
	}
}

func (key WorkflowKey) Proto() *temporalessv1.WorkflowKey {
	key = key.withDefaults()
	return &temporalessv1.WorkflowKey{
		Namespace:  key.Namespace,
		WorkflowId: key.WorkflowID,
		RunId:      key.RunID,
	}
}

func (key ActivityKey) Proto() *temporalessv1.ActivityKey {
	key = key.withDefaults()
	return &temporalessv1.ActivityKey{
		Namespace:  key.Namespace,
		WorkflowId: key.WorkflowID,
		RunId:      key.RunID,
		ActivityId: key.ActivityID,
	}
}

func (key TimerKey) Proto() *temporalessv1.TimerKey {
	key = key.withDefaults()
	return &temporalessv1.TimerKey{
		Namespace:  key.Namespace,
		WorkflowId: key.WorkflowID,
		RunId:      key.RunID,
		TimerId:    key.TimerID,
	}
}

func (key ClaimKey) Proto() *temporalessv1.ClaimKey {
	key = key.withDefaults()
	return &temporalessv1.ClaimKey{
		Namespace:  key.Namespace,
		WorkflowId: key.WorkflowID,
		RunId:      key.RunID,
		ClaimId:    key.ClaimID,
	}
}

func (key EventKey) Proto() *temporalessv1.EventKey {
	key = key.withDefaults()
	return &temporalessv1.EventKey{
		Namespace:  key.Namespace,
		WorkflowId: key.WorkflowID,
		RunId:      key.RunID,
		EventId:    key.EventID,
	}
}

// Storage paths are strict Hive partition style:
//
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=activity/activity_id={aid}/record.binpb
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=timer/timer_id={tid}/record.binpb
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=event/event_id={eid}/record.binpb
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=claim/claim_id={cid}/record.binpb
//
// Every directory level is a Hive partition column. Spark/Trino/DuckDB pointed
// at temporaless/v1/ auto-discovers `namespace`, `workflow_id`, `run_id`,
// `kind`, plus the per-kind id column.
const StorageRootPrefix = "temporaless/v1"

func runPrefix(namespace, workflowID, runID string) string {
	return fmt.Sprintf(
		"%s/namespace=%s/workflow_id=%s/run_id=%s",
		StorageRootPrefix,
		namespace,
		workflowID,
		runID,
	)
}

func (key WorkflowKey) Path() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/kind=workflow/record.binpb", nil
}

// DirPath returns the run's root partition (everything for this workflow run
// lives under it, across all kinds). Useful for "delete the whole run" or
// "list all records for this run".
func (key WorkflowKey) DirPath() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/", nil
}

func (key ActivityKey) Path() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s/kind=activity/activity_id=%s/record.binpb",
		runPrefix(key.Namespace, key.WorkflowID, key.RunID),
		key.ActivityID,
	), nil
}

func (key TimerKey) Path() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s/kind=timer/timer_id=%s/record.binpb",
		runPrefix(key.Namespace, key.WorkflowID, key.RunID),
		key.TimerID,
	), nil
}

func (key ClaimKey) Path() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s/kind=claim/claim_id=%s/record.binpb",
		runPrefix(key.Namespace, key.WorkflowID, key.RunID),
		key.ClaimID,
	), nil
}

// DirPath returns the kind-partition prefix for activities. Listing under it
// yields all activity records for the run.
func (key ActivityKey) DirPath() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/kind=activity/", nil
}

func (key TimerKey) DirPath() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/kind=timer/", nil
}

func (key ClaimKey) DirPath() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/kind=claim/", nil
}

func (key EventKey) Path() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"%s/kind=event/event_id=%s/record.binpb",
		runPrefix(key.Namespace, key.WorkflowID, key.RunID),
		key.EventID,
	), nil
}

func (key EventKey) DirPath() (string, error) {
	key = key.withDefaults()
	if err := key.Validate(); err != nil {
		return "", err
	}
	return runPrefix(key.Namespace, key.WorkflowID, key.RunID) + "/kind=event/", nil
}

func (key WorkflowKey) Validate() error {
	key = key.withDefaults()
	return protovalidate.Validate(key.Proto())
}

func (key ActivityKey) Validate() error {
	key = key.withDefaults()
	return protovalidate.Validate(key.Proto())
}

func (key TimerKey) Validate() error {
	key = key.withDefaults()
	return protovalidate.Validate(key.Proto())
}

func (key ClaimKey) Validate() error {
	key = key.withDefaults()
	return protovalidate.Validate(key.Proto())
}

func (key EventKey) Validate() error {
	key = key.withDefaults()
	return protovalidate.Validate(key.Proto())
}

func (key WorkflowKey) withDefaults() WorkflowKey {
	if key.Namespace == "" {
		key.Namespace = DefaultNamespace
	}
	return key
}

func (key ActivityKey) withDefaults() ActivityKey {
	if key.Namespace == "" {
		key.Namespace = DefaultNamespace
	}
	return key
}

func (key TimerKey) withDefaults() TimerKey {
	if key.Namespace == "" {
		key.Namespace = DefaultNamespace
	}
	return key
}

func (key ClaimKey) withDefaults() ClaimKey {
	if key.Namespace == "" {
		key.Namespace = DefaultNamespace
	}
	return key
}

func (key EventKey) withDefaults() EventKey {
	if key.Namespace == "" {
		key.Namespace = DefaultNamespace
	}
	return key
}
