package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
)

type OpenDALStore struct {
	operator *opendal.Operator
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
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) GetTimer(ctx context.Context, key TimerKey) (*temporalessv1.TimerRecord, bool, error) {
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
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) PutActivity(ctx context.Context, record *temporalessv1.ActivityRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := ActivityKeyFromProto(record.GetKey())
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
		return nil, false, err
	}
	return record, true, nil
}

func (store *OpenDALStore) PutEvent(ctx context.Context, record *temporalessv1.EventRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := EventKeyFromProto(record.GetKey())
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

func (store *OpenDALStore) ListWorkflows(
	ctx context.Context,
	namespace string,
	workflowID string,
	status temporalessv1.WorkflowStatus,
) ([]*temporalessv1.WorkflowRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := StorageRootPrefix + "/"
	if namespace != "" {
		root = root + "namespace=" + namespace + "/"
		if workflowID != "" {
			root = root + "workflow_id=" + workflowID + "/"
		}
	}
	paths, err := walkOpenDAL(ctx, store.operator, root)
	if err != nil {
		return nil, err
	}

	// When the path can't fully encode the filter (empty namespace + non-empty
	// workflowID), apply the workflow_id filter in code as defense-in-depth.
	matchWorkflowID := ""
	if namespace == "" && workflowID != "" {
		matchWorkflowID = workflowID
	}
	var records []*temporalessv1.WorkflowRecord
	for _, path := range paths {
		if !strings.HasSuffix(path, "/kind=workflow/record.binpb") {
			continue
		}
		key, ok := parseWorkflowPath(path)
		if !ok {
			continue
		}
		if matchWorkflowID != "" && key.WorkflowID != matchWorkflowID {
			continue
		}
		record, found, err := store.GetWorkflow(ctx, key)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if status != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && record.GetStatus() != status {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

func (store *OpenDALStore) DeleteWorkflow(ctx context.Context, key WorkflowKey) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := key.Path()
	if err != nil {
		return false, err
	}
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
		activityID, ok := extractIDFromHivePath(path, dir, "activity_id=")
		if !ok {
			continue
		}
		record, found, err := store.GetActivity(ctx, ActivityKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			ActivityID: activityID,
		})
		if err != nil {
			return nil, err
		}
		if !found {
			continue
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
	var records []*temporalessv1.TimerRecord
	for _, path := range paths {
		timerID, ok := extractIDFromHivePath(path, dir, "timer_id=")
		if !ok {
			continue
		}
		record, found, err := store.GetTimer(ctx, TimerKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			TimerID:    timerID,
		})
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		if status != temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED && record.GetStatus() != status {
			continue
		}
		records = append(records, record)
	}
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
	return deleteIfExists(store.operator, path)
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
		eventID, ok := extractIDFromHivePath(path, dir, "event_id=")
		if !ok {
			continue
		}
		record, found, err := store.GetEvent(ctx, EventKey{
			Namespace:  key.Namespace,
			WorkflowID: key.WorkflowID,
			RunID:      key.RunID,
			EventID:    eventID,
		})
		if err != nil {
			return nil, err
		}
		if !found {
			continue
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

func (store *OpenDALStore) Sweep(ctx context.Context, namespace string, now time.Time, maxAge time.Duration) (uint32, error) {
	if maxAge <= 0 {
		return 0, fmt.Errorf("max_age must be > 0")
	}
	cutoff := now.Add(-maxAge)
	completed, err := store.ListWorkflows(ctx, namespace, "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	if err != nil {
		return 0, err
	}
	var deleted uint32
	for _, record := range completed {
		if !record.GetCompletedAt().AsTime().Before(cutoff) && !record.GetCompletedAt().AsTime().Equal(cutoff) {
			continue
		}
		key := WorkflowKeyFromProto(record.GetKey())
		activities, err := store.ListActivities(ctx, key)
		if err != nil {
			return deleted, err
		}
		for _, activity := range activities {
			if _, err := store.DeleteActivity(ctx, ActivityKeyFromProto(activity.GetKey())); err != nil {
				return deleted, err
			}
		}
		timers, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
		if err != nil {
			return deleted, err
		}
		for _, timer := range timers {
			if _, err := store.DeleteTimer(ctx, TimerKeyFromProto(timer.GetKey())); err != nil {
				return deleted, err
			}
		}
		events, err := store.ListEvents(ctx, key)
		if err != nil {
			return deleted, err
		}
		for _, event := range events {
			if _, err := store.DeleteEvent(ctx, EventKeyFromProto(event.GetKey())); err != nil {
				return deleted, err
			}
		}
		if _, err := store.DeleteWorkflow(ctx, key); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (store *OpenDALStore) DueTimers(ctx context.Context, namespace string, now time.Time) ([]DueTimer, error) {
	inFlight, err := store.ListWorkflows(ctx, namespace, "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS)
	if err != nil {
		return nil, err
	}
	var due []DueTimer
	for _, workflow := range inFlight {
		key := WorkflowKeyFromProto(workflow.GetKey())
		timers, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
		if err != nil {
			return due, err
		}
		for _, timer := range timers {
			if timer.GetFireAt().AsTime().After(now) {
				continue
			}
			due = append(due, DueTimer{
				Key:      TimerKeyFromProto(timer.GetKey()),
				Record:   timer,
				Workflow: workflow,
			})
		}
	}
	return due, nil
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
	return files, nil
}

// parseWorkflowPath inverts the Hive layout for the workflow record:
//
//	temporaless/v1/namespace={ns}/workflow_id={wf}/run_id={rid}/kind=workflow/record.binpb
func parseWorkflowPath(path string) (WorkflowKey, bool) {
	var zero WorkflowKey
	parts := strings.Split(path, "/")
	if len(parts) != 7 {
		return zero, false
	}
	if parts[0] != "temporaless" || parts[1] != "v1" {
		return zero, false
	}
	if parts[5] != "kind=workflow" || parts[6] != "record.binpb" {
		return zero, false
	}
	namespace, ok := stripPrefix(parts[2], "namespace=")
	if !ok {
		return zero, false
	}
	workflowID, ok := stripPrefix(parts[3], "workflow_id=")
	if !ok {
		return zero, false
	}
	runID, ok := stripPrefix(parts[4], "run_id=")
	if !ok {
		return zero, false
	}
	return WorkflowKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}, true
}

func stripPrefix(s, prefix string) (string, bool) {
	if !strings.HasPrefix(s, prefix) {
		return "", false
	}
	return s[len(prefix):], true
}

// extractIDFromHivePath parses the per-kind ID out of a record path. With
// `dir` set to the kind partition (e.g. ".../kind=activity/") and
// `idPrefix` to the column name (e.g. "activity_id="), the path inside
// `dir` is `{idPrefix}{value}/record.binpb`.
func extractIDFromHivePath(path, dir, idPrefix string) (string, bool) {
	if !strings.HasSuffix(path, "/record.binpb") {
		return "", false
	}
	rel, ok := strings.CutPrefix(path, dir)
	if !ok {
		return "", false
	}
	rel = strings.TrimSuffix(rel, "/record.binpb")
	return strings.CutPrefix(rel, idPrefix)
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
	key := TimerKeyFromProto(record.GetKey())
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
