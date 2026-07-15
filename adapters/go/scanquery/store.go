// Package scanquery provides an explicit offline/development query adapter
// over an OpenDAL bucket. It is intentionally not part of core storage and is
// not a production search index: every broad query walks object storage.
package scanquery

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

var _ storage.QueryStore = (*Store)(nil)

// Store scans an OpenDAL bucket for offline/development queries, then uses the
// supplied point store for bounded run reads and deletion. operator and point
// must address the same bucket.
type Store struct {
	operator *opendal.Operator
	point    storage.Store
	claims   storage.ClaimStore
}

func New(operator *opendal.Operator, point storage.Store, claims storage.ClaimStore) (*Store, error) {
	if operator == nil {
		return nil, fmt.Errorf("operator is required")
	}
	if point == nil {
		return nil, fmt.Errorf("point store is required")
	}
	return &Store{operator: operator, point: point, claims: claims}, nil
}

func (store *Store) ListWorkflows(
	ctx context.Context,
	request *temporalessv1.ListWorkflowsRequest,
) (*temporalessv1.ListWorkflowsResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: list workflows request is required", storage.ErrInvalidQuery)
	}
	if err := rejectPagination(request.GetOrderBy(), request.GetPageSize(), request.GetPageToken()); err != nil {
		return nil, err
	}

	namespace := request.GetNamespace()
	workflowID := request.GetWorkflowId()
	if err := validateQueryScope(namespace, workflowID); err != nil {
		return nil, err
	}
	root := storage.StorageRootPrefix + "/"
	if namespace != "" {
		root += namespace + "/"
		if workflowID != "" {
			root += workflowID + "/"
		}
	}
	paths, err := walk(ctx, store.operator, root)
	if err != nil {
		return nil, err
	}

	records := make([]*temporalessv1.WorkflowRecord, 0)
	for _, path := range paths {
		if !strings.HasSuffix(path, "/workflow.binpb") {
			continue
		}
		record := &temporalessv1.WorkflowRecord{}
		if err := readProto(ctx, store.operator, path, record); err != nil {
			return nil, err
		}
		key := storage.WorkflowKeyFromProto(record.GetKey())
		if err := storage.ValidateWorkflowRecord(record, key); err != nil {
			return nil, err
		}
		if err := validatePayloadLocation("workflow", path, key.Path, key.Validate()); err != nil {
			return nil, err
		}
		if namespace != "" && namespaceOrDefault(key.Namespace) != namespace {
			continue
		}
		if workflowID != "" && key.WorkflowID != workflowID {
			continue
		}
		if status := request.GetStatus(); status != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && record.GetStatus() != status {
			continue
		}
		records = append(records, record)
	}
	return &temporalessv1.ListWorkflowsResponse{Records: records}, nil
}

func (store *Store) ListActivitiesQuery(
	ctx context.Context,
	request *temporalessv1.RecordQueryServiceListActivitiesRequest,
) (*temporalessv1.RecordQueryServiceListActivitiesResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("%w: list activities request is required", storage.ErrInvalidQuery)
	}
	if err := rejectPagination(request.GetOrderBy(), request.GetPageSize(), request.GetPageToken()); err != nil {
		return nil, err
	}
	namespace := request.GetNamespace()
	workflowID := request.GetWorkflowId()
	runID := request.GetRunId()
	if runID != "" && workflowID == "" {
		return nil, fmt.Errorf("%w: workflow_id is required when run_id is set", storage.ErrInvalidQuery)
	}
	if err := validateQueryScope(namespace, workflowID); err != nil {
		return nil, err
	}

	if runID != "" {
		key := storage.WorkflowKey{Namespace: namespaceOrDefault(namespace), WorkflowID: workflowID, RunID: runID}
		if err := key.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %w", storage.ErrInvalidQuery, err)
		}
		records, err := store.point.ListActivities(ctx, key)
		if err != nil {
			return nil, err
		}
		return &temporalessv1.RecordQueryServiceListActivitiesResponse{
			Records: filterActivities(records, request.GetStatus()),
		}, nil
	}

	root := storage.StorageRootPrefix + "/"
	if namespace != "" {
		root += namespace + "/"
		if workflowID != "" {
			root += workflowID + "/"
		}
	}
	paths, err := walk(ctx, store.operator, root)
	if err != nil {
		return nil, err
	}
	records := make([]*temporalessv1.ActivityRecord, 0)
	for _, path := range paths {
		if !strings.Contains(path, "/activity/") || !strings.HasSuffix(path, ".binpb") {
			continue
		}
		record := &temporalessv1.ActivityRecord{}
		if err := readProto(ctx, store.operator, path, record); err != nil {
			return nil, err
		}
		key := storage.ActivityKeyFromProto(record.GetKey())
		if err := storage.ValidateActivityRecord(record, key); err != nil {
			return nil, err
		}
		if err := validatePayloadLocation("activity", path, key.Path, key.Validate()); err != nil {
			return nil, err
		}
		if namespace != "" && namespaceOrDefault(key.Namespace) != namespace {
			continue
		}
		if workflowID != "" && key.WorkflowID != workflowID {
			continue
		}
		if status := request.GetStatus(); status != temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED && record.GetStatus() != status {
			continue
		}
		records = append(records, record)
	}
	return &temporalessv1.RecordQueryServiceListActivitiesResponse{Records: records}, nil
}

func (store *Store) Sweep(ctx context.Context, request *temporalessv1.SweepRequest) (*temporalessv1.SweepResponse, error) {
	if request == nil || request.GetNow() == nil || request.GetNow().CheckValid() != nil || request.GetMaxAge() == nil || request.GetMaxAge().CheckValid() != nil || request.GetMaxAge().AsDuration() <= 0 {
		return nil, fmt.Errorf("%w: sweep requires now and max_age > 0", storage.ErrInvalidQuery)
	}
	deleted, err := janitor.Sweep(ctx, store, store.point, store.claims, request)
	if err != nil {
		return nil, err
	}
	return &temporalessv1.SweepResponse{Deleted: deleted}, nil
}

func (store *Store) DueTimersQuery(
	ctx context.Context,
	request *temporalessv1.RecordQueryServiceDueTimersRequest,
) (*temporalessv1.RecordQueryServiceDueTimersResponse, error) {
	if request == nil || request.GetNow() == nil {
		return nil, fmt.Errorf("%w: due timers request and now are required", storage.ErrInvalidQuery)
	}
	due, err := store.point.DueTimers(ctx, request.GetNamespace(), request.GetNow().AsTime())
	if err != nil {
		return nil, err
	}
	response := &temporalessv1.RecordQueryServiceDueTimersResponse{
		Due: make([]*temporalessv1.DueTimer, 0, len(due)),
	}
	for _, entry := range due {
		response.Due = append(response.Due, &temporalessv1.DueTimer{
			Key:      entry.Key.Proto(),
			Record:   entry.Record,
			Workflow: entry.Workflow,
		})
	}
	return response, nil
}

func filterActivities(records []*temporalessv1.ActivityRecord, status temporalessv1.ActivityStatus) []*temporalessv1.ActivityRecord {
	if status == temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED {
		return records
	}
	filtered := make([]*temporalessv1.ActivityRecord, 0, len(records))
	for _, record := range records {
		if record.GetStatus() == status {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func rejectPagination(orderBy string, pageSize int32, pageToken string) error {
	if orderBy != "" || pageSize != 0 || pageToken != "" {
		return fmt.Errorf("%w: scan query does not support order_by or pagination", storage.ErrInvalidQuery)
	}
	return nil
}

func validateQueryScope(namespace string, workflowID string) error {
	probeNamespace := namespaceOrDefault(namespace)
	probeWorkflowID := workflowID
	if probeWorkflowID == "" {
		probeWorkflowID = "placeholder"
	}
	if err := (storage.WorkflowKey{
		Namespace:  probeNamespace,
		WorkflowID: probeWorkflowID,
		RunID:      "placeholder",
	}).Validate(); err != nil {
		return fmt.Errorf("%w: %w", storage.ErrInvalidQuery, err)
	}
	return nil
}

func namespaceOrDefault(namespace string) string {
	if namespace == "" {
		return storage.DefaultNamespace
	}
	return namespace
}

type pathBuilder func() (string, error)

func validatePayloadLocation(kind string, actual string, expectedPath pathBuilder, validateErr error) error {
	if validateErr != nil {
		return fmt.Errorf("%w: invalid %s payload key at %s: %w", storage.ErrCorruptRecord, kind, actual, validateErr)
	}
	expected, err := expectedPath()
	if err != nil {
		return err
	}
	if expected != actual {
		return fmt.Errorf("%w: %s payload key does not match its storage location at %s", storage.ErrCorruptRecord, kind, actual)
	}
	return nil
}

func walk(ctx context.Context, operator *opendal.Operator, root string) ([]string, error) {
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
			if isNotFound(err) {
				continue
			}
			return nil, err
		}
		for lister.Next() {
			path := lister.Entry().Path()
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
		if err := lister.Error(); err != nil {
			return nil, err
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	sort.Strings(files)
	return files, nil
}

func readProto(ctx context.Context, operator *opendal.Operator, path string, message proto.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := operator.Read(path)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return fmt.Errorf("decode listed protobuf %s: %w", path, err)
	}
	return nil
}

func isNotFound(err error) bool {
	var openDALError *opendal.Error
	return errors.As(err, &openDALError) && openDALError.Code() == opendal.CodeNotFound
}
