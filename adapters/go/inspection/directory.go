package inspection

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var idPrefixPattern = regexp.MustCompile(`^[A-Za-z0-9._:=-]*$`)

// ListWorkflowDirectory implements RunInspectionService. It lists the
// namespace's latest-run pointers by name, reads at most Limits.ReadBudget of
// them per request, and resolves each against its authoritative workflow
// record. Pending state is derived for runs still in progress.
func (service *Service) ListWorkflowDirectory(
	ctx context.Context,
	request *inspectionv1.ListWorkflowDirectoryRequest,
) (*inspectionv1.ListWorkflowDirectoryResponse, error) {
	store, _, err := service.namespaceScope(ctx, request.GetStore(), request.GetNamespace())
	if err != nil {
		return nil, err
	}
	prefix := request.GetWorkflowIdPrefix()
	if !idPrefixPattern.MatchString(prefix) {
		return nil, status.Error(codes.InvalidArgument, "workflow_id_prefix may contain only letters, digits, and . _ : = -")
	}
	namespace := request.GetNamespace()
	binding := fmt.Sprintf("directory\x1f%s\x1f%s\x1f%s\x1f%d", store.ID, namespace, prefix, request.GetStatus())
	cursor, err := service.openCursor(ctx, binding, request.GetPageToken())
	if err != nil {
		return nil, err
	}

	dir := latestDir(namespace)
	names, err := store.Bucket.List(ctx, dir)
	if err != nil {
		return nil, service.storeError(err)
	}
	if len(names) > service.limits.MaxListedObjects {
		return nil, service.storeError(fmt.Errorf("%w: %d latest-run pointers in namespace %q", errTooManyObjects, len(names), namespace))
	}
	// Names are constructed from workflow IDs, so a prefix filter compares
	// constructed paths; identity is still read from each payload.
	candidates := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, ".binpb") && name > cursor && strings.HasPrefix(name, dir+prefix) {
			candidates = append(candidates, name)
		}
	}

	pageSize := service.pageSize(request.GetPageSize())
	budget := service.limits.ReadBudget
	response := &inspectionv1.ListWorkflowDirectoryResponse{}
	next := 0
	lastProcessed := ""
	for next < len(candidates) && len(response.Entries) < pageSize && int(response.Scanned) < budget {
		batch := candidates[next:min(next+service.limits.Concurrency, len(candidates), next+budget-int(response.Scanned))]
		entries := make([]*inspectionv1.WorkflowDirectoryEntry, len(batch))
		if err := forEach(ctx, service.limits.Concurrency, batch, func(ctx context.Context, index int, path string) error {
			entry, err := service.directoryEntry(ctx, store, namespace, path)
			entries[index] = entry
			return err
		}); err != nil {
			return nil, service.storeError(err)
		}
		for index, entry := range entries {
			response.Scanned++
			lastProcessed = batch[index]
			next++
			if entry == nil || !directoryStatusMatches(entry, request.GetStatus()) {
				continue
			}
			response.Entries = append(response.Entries, entry)
			if len(response.Entries) == pageSize {
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

func directoryStatusMatches(entry *inspectionv1.WorkflowDirectoryEntry, want temporalessv1.WorkflowStatus) bool {
	if want == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED {
		return true
	}
	if entry.GetRun() != nil {
		return entry.GetRun().GetStatus() == want
	}
	return entry.GetPointer().GetStatus() == want
}

// directoryEntry resolves one pointer object. A pointer that vanished, cannot
// be decoded, or is stored at the wrong key yields nil: it has no identity
// inspection can trust.
func (service *Service) directoryEntry(
	ctx context.Context,
	store *Store,
	namespace string,
	path string,
) (*inspectionv1.WorkflowDirectoryEntry, error) {
	data, found, err := store.Bucket.Read(ctx, path)
	if err != nil || !found {
		return nil, err
	}
	pointer := &temporalessv1.LatestWorkflowRunPointer{}
	if err := proto.Unmarshal(data, pointer); err != nil {
		service.logger.Warn("inspection: undecodable latest-run pointer", "store", store.ID, "path", path, "error", err)
		return nil, nil
	}
	key := storage.WorkflowKeyFromProto(pointer.GetKey())
	if err := storage.ValidateLatestWorkflowRunPointer(pointer, namespace, key.WorkflowID); err != nil ||
		latestPath(namespace, key.WorkflowID) != path {
		service.logger.Warn("inspection: invalid latest-run pointer", "store", store.ID, "path", path, "error", err)
		return nil, nil
	}

	entry := &inspectionv1.WorkflowDirectoryEntry{Pointer: pointer}
	record, found, err := store.Records.GetWorkflow(ctx, key)
	switch {
	case errors.Is(err, storage.ErrCorruptRecord):
		entry.StalePointer = true
		return entry, nil
	case err != nil:
		return nil, err
	case !found:
		entry.StalePointer = true
		return entry, nil
	}
	entry.Run = runSummary(record)
	entry.StalePointer = storage.ValidateLatestWorkflowRunReference(pointer, record) != nil

	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		entry.Pending = DerivePending(RunRecords{Workflow: record}, service.now(), overdueGrace(store))
		return entry, nil
	}
	records, _, err := service.readRun(ctx, store, key, record)
	if err != nil {
		if errors.Is(err, storage.ErrCorruptRecord) {
			service.logger.Warn("inspection: corrupt run records", "store", store.ID, "run", key, "error", err)
			return entry, nil
		}
		return nil, err
	}
	entry.Pending = DerivePending(records, service.now(), overdueGrace(store))
	return entry, nil
}

func runSummary(record *temporalessv1.WorkflowRecord) *inspectionv1.WorkflowRunSummary {
	return &inspectionv1.WorkflowRunSummary{
		Key:          record.GetKey(),
		WorkflowType: record.GetWorkflowType(),
		Status:       record.GetStatus(),
		Failure:      record.GetFailure(),
		CreatedAt:    record.GetCreatedAt(),
		CompletedAt:  record.GetCompletedAt(),
		RunOrderTime: record.GetRunOrderTime(),
		Annotations:  record.GetAnnotations(),
	}
}

// ListWorkflowRuns implements RunInspectionService with a bounded,
// non-recursive listing of one workflow_id's run directories.
func (service *Service) ListWorkflowRuns(
	ctx context.Context,
	request *inspectionv1.ListWorkflowRunsRequest,
) (*inspectionv1.ListWorkflowRunsResponse, error) {
	store, _, err := service.namespaceScope(ctx, request.GetStore(), request.GetNamespace())
	if err != nil {
		return nil, err
	}
	namespace := request.GetNamespace()
	workflowID := request.GetWorkflowId()
	if err := validateScopeSegments(namespace, workflowID); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid workflow_id: %v", err)
	}
	binding := fmt.Sprintf("runs\x1f%s\x1f%s\x1f%s\x1f%t", store.ID, namespace, workflowID, request.GetAscending())
	cursor, err := service.openCursor(ctx, binding, request.GetPageToken())
	if err != nil {
		return nil, err
	}

	children, err := store.Bucket.List(ctx, workflowDir(namespace, workflowID))
	if err != nil {
		return nil, service.storeError(err)
	}
	runDirs := make([]string, 0, len(children))
	for _, child := range children {
		if strings.HasSuffix(child, "/") {
			runDirs = append(runDirs, child)
		}
	}
	if len(runDirs) > service.limits.MaxListedRuns {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"workflow %q has %d runs, more than the %d a bucket listing serves; use an indexed query store",
			workflowID, len(runDirs), service.limits.MaxListedRuns,
		)
	}
	if !request.GetAscending() {
		sort.Sort(sort.Reverse(sort.StringSlice(runDirs)))
	}
	start := 0
	if cursor != "" {
		start = sort.Search(len(runDirs), func(index int) bool {
			if request.GetAscending() {
				return runDirs[index] > cursor
			}
			return runDirs[index] < cursor
		})
	}
	page := runDirs[start:min(start+service.pageSize(request.GetPageSize()), len(runDirs))]

	summaries := make([]*inspectionv1.WorkflowRunSummary, len(page))
	if err := forEach(ctx, service.limits.Concurrency, page, func(ctx context.Context, index int, dir string) error {
		summary, err := service.runDirSummary(ctx, store, namespace, workflowID, dir)
		summaries[index] = summary
		return err
	}); err != nil {
		return nil, service.storeError(err)
	}
	response := &inspectionv1.ListWorkflowRunsResponse{TotalListed: uint32(len(runDirs))}
	for _, summary := range summaries {
		if summary == nil {
			response.RunsWithoutWorkflowRecord++
			continue
		}
		response.Runs = append(response.Runs, summary)
	}
	if start+len(page) < len(runDirs) && len(page) > 0 {
		if response.NextPageToken, err = service.sealCursor(ctx, binding, page[len(page)-1]); err != nil {
			return nil, err
		}
	}
	return response, nil
}

// runDirSummary reads the workflow record inside one listed run directory. It
// accepts the record only when the payload's own key constructs that path.
func (service *Service) runDirSummary(
	ctx context.Context,
	store *Store,
	namespace string,
	workflowID string,
	dir string,
) (*inspectionv1.WorkflowRunSummary, error) {
	path := dir + "workflow.binpb"
	data, found, err := store.Bucket.Read(ctx, path)
	if err != nil || !found {
		return nil, err
	}
	record := &temporalessv1.WorkflowRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		service.logger.Warn("inspection: undecodable workflow record", "store", store.ID, "path", path, "error", err)
		return nil, nil
	}
	key := storage.WorkflowKeyFromProto(record.GetKey())
	expected, pathErr := key.Path()
	if err := storage.ValidateWorkflowRecord(record, key); err != nil || pathErr != nil ||
		expected != path || key.Namespace != namespace || key.WorkflowID != workflowID {
		service.logger.Warn("inspection: misplaced workflow record", "store", store.ID, "path", path)
		return nil, nil
	}
	return runSummary(record), nil
}
