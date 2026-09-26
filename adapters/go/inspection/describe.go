package inspection

import (
	"context"
	"fmt"
	"sort"
	"strings"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// DescribeRun implements RunInspectionService. It composes point reads and
// run-scoped listings; the result is not a transactional snapshot.
func (service *Service) DescribeRun(
	ctx context.Context,
	request *inspectionv1.DescribeRunRequest,
) (*inspectionv1.DescribeRunResponse, error) {
	key := storage.WorkflowKeyFromProto(request.GetKey())
	store, grant, err := service.namespaceScope(ctx, request.GetStore(), request.GetKey().GetNamespace())
	if err != nil {
		return nil, err
	}
	if err := key.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid run key: %v", err)
	}

	workflow, found, err := store.Records.GetWorkflow(ctx, key)
	if err != nil {
		return nil, service.storeError(err)
	}
	if !found {
		workflow = nil
	}
	records, truncated, err := service.readRun(ctx, store, key, workflow)
	if err != nil {
		return nil, service.storeError(err)
	}
	if records.Workflow == nil && len(records.Activities) == 0 && len(records.Timers) == 0 &&
		len(records.Events) == 0 && len(records.Claims) == 0 {
		return nil, status.Errorf(codes.NotFound, "run %s/%s/%s has no records", key.Namespace, key.WorkflowID, key.RunID)
	}

	response := &inspectionv1.DescribeRunResponse{
		ClaimsInspected:   records.ClaimsInspected,
		History:           DeriveHistory(records),
		Pending:           DerivePending(records, service.now(), overdueGrace(store)),
		ObservedAt:        timestamppb.New(service.now()),
		Truncated:         truncated,
		PayloadVisibility: inspectionv1.PayloadVisibility_PAYLOAD_VISIBILITY_VISIBLE,
	}
	if !grant.Payloads {
		response.PayloadVisibility = inspectionv1.PayloadVisibility_PAYLOAD_VISIBILITY_REDACTED
	}
	renderer := store.Payloads
	if renderer == nil {
		renderer = service.renderer
	}
	render := func(path string, payload *anypb.Any) (*anypb.Any, error) {
		if payload == nil {
			return nil, nil
		}
		response.Payloads = append(response.Payloads, renderer.Render(path, payload, grant.Payloads))
		return recordPayload(payload, grant.Payloads)
	}

	if records.Workflow != nil {
		workflow := proto.Clone(records.Workflow).(*temporalessv1.WorkflowRecord)
		if workflow.Input, err = render("workflow.input", workflow.GetInput()); err != nil {
			return nil, err
		}
		if workflow.Result, err = render("workflow.result", workflow.GetResult()); err != nil {
			return nil, err
		}
		response.Workflow = workflow
	}
	for _, record := range records.Activities {
		activity := proto.Clone(record).(*temporalessv1.ActivityRecord)
		prefix := "activity/" + activity.GetKey().GetActivityId()
		if activity.Input, err = render(prefix+".input", activity.GetInput()); err != nil {
			return nil, err
		}
		if activity.Result, err = render(prefix+".result", activity.GetResult()); err != nil {
			return nil, err
		}
		response.Activities = append(response.Activities, activity)
	}
	for _, record := range records.Events {
		event := proto.Clone(record).(*temporalessv1.EventRecord)
		if event.Payload, err = render("event/"+event.GetKey().GetEventId()+".payload", event.GetPayload()); err != nil {
			return nil, err
		}
		response.Events = append(response.Events, event)
	}
	response.Timers = records.Timers
	response.Claims = records.Claims
	return response, nil
}

// recordPayload is the Any a response record carries for one stored payload.
// A visible payload stays as stored when ProtoJSON can render it with the
// types this process resolves (protoregistry.GlobalTypes: well-known types,
// linked types, and registered descriptors), which is exactly what the
// Connect/HTTP JSON, MCP, and CLI projections marshal with. Anything else,
// and every redacted payload, becomes an OpaquePayload, so no projection
// fails to marshal the response.
func recordPayload(payload *anypb.Any, visible bool) (*anypb.Any, error) {
	if visible {
		if _, err := protojson.Marshal(payload); err == nil {
			return payload, nil
		}
	}
	opaque := &inspectionv1.OpaquePayload{TypeUrl: payload.GetTypeUrl(), Redacted: !visible}
	if visible {
		opaque.Value = payload.GetValue()
	}
	wrapped, err := anypb.New(opaque)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "wrap payload: %v", err)
	}
	return wrapped, nil
}

// readRun reads one run's child records, at most Limits.MaxRunRecords per
// kind, in parallel. Timers come from the point store so the due ledger's
// write-ahead copies are honored exactly as the runtime sees them.
func (service *Service) readRun(
	ctx context.Context,
	store *Store,
	key storage.WorkflowKey,
	workflow *temporalessv1.WorkflowRecord,
) (RunRecords, bool, error) {
	limit := service.limits.MaxRunRecords
	records := RunRecords{Workflow: workflow, ClaimsInspected: store.ListClaims}
	truncated := false

	runDir, err := key.DirPath()
	if err != nil {
		return RunRecords{}, false, err
	}
	activities, cut, err := readRunScoped(ctx, service, store, runDir+"activity/", limit,
		func() *temporalessv1.ActivityRecord { return &temporalessv1.ActivityRecord{} },
		func(record *temporalessv1.ActivityRecord) (string, error) {
			recordKey := storage.ActivityKeyFromProto(record.GetKey())
			if err := storage.ValidateActivityRecord(record, recordKey); err != nil {
				return "", err
			}
			return sameRunPath(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID, recordKey.Path)
		})
	if err != nil {
		return RunRecords{}, false, err
	}
	truncated = truncated || cut
	records.Activities = activities
	sort.Slice(records.Activities, func(left, right int) bool {
		return records.Activities[left].GetKey().GetActivityId() < records.Activities[right].GetKey().GetActivityId()
	})

	events, cut, err := readRunScoped(ctx, service, store, runDir+"event/", limit,
		func() *temporalessv1.EventRecord { return &temporalessv1.EventRecord{} },
		func(record *temporalessv1.EventRecord) (string, error) {
			recordKey := storage.EventKeyFromProto(record.GetKey())
			if err := storage.ValidateEventRecord(record, recordKey); err != nil {
				return "", err
			}
			return sameRunPath(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID, recordKey.Path)
		})
	if err != nil {
		return RunRecords{}, false, err
	}
	truncated = truncated || cut
	records.Events = events
	sort.Slice(records.Events, func(left, right int) bool {
		return records.Events[left].GetKey().GetEventId() < records.Events[right].GetKey().GetEventId()
	})

	if store.ListClaims {
		claims, cut, err := readRunScoped(ctx, service, store, runDir+"claim/", limit,
			func() *temporalessv1.ClaimRecord { return &temporalessv1.ClaimRecord{} },
			func(record *temporalessv1.ClaimRecord) (string, error) {
				recordKey := storage.ClaimKeyFromProto(record.GetKey())
				if err := storage.ValidateClaimRecord(record, recordKey); err != nil {
					return "", err
				}
				return sameRunPath(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID, recordKey.Path)
			})
		if err != nil {
			return RunRecords{}, false, err
		}
		truncated = truncated || cut
		records.Claims = claims
		sort.Slice(records.Claims, func(left, right int) bool {
			return records.Claims[left].GetKey().GetClaimId() < records.Claims[right].GetKey().GetClaimId()
		})
	}

	timers, err := store.Records.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		return RunRecords{}, false, err
	}
	sort.Slice(timers, func(left, right int) bool {
		return timers[left].GetKey().GetTimerId() < timers[right].GetKey().GetTimerId()
	})
	if len(timers) > limit {
		timers = timers[:limit]
		truncated = true
	}
	records.Timers = timers
	return records, truncated, nil
}

// readRunScoped lists one run-scoped record directory and reads the first
// limit objects. check validates a decoded record and returns the path its
// own key constructs, which must equal the path it was read from.
func readRunScoped[T proto.Message](
	ctx context.Context,
	service *Service,
	store *Store,
	dir string,
	limit int,
	newRecord func() T,
	check func(T) (string, error),
) ([]T, bool, error) {
	children, err := store.Bucket.List(ctx, dir)
	if err != nil {
		return nil, false, err
	}
	paths := make([]string, 0, len(children))
	for _, child := range children {
		if strings.HasSuffix(child, ".binpb") {
			paths = append(paths, child)
		}
	}
	truncated := len(paths) > limit
	if truncated {
		paths = paths[:limit]
	}
	records := make([]T, len(paths))
	found := make([]bool, len(paths))
	if err := forEach(ctx, service.limits.Concurrency, paths, func(ctx context.Context, index int, path string) error {
		data, ok, err := store.Bucket.Read(ctx, path)
		if err != nil || !ok {
			return err
		}
		record := newRecord()
		if err := proto.Unmarshal(data, record); err != nil {
			return fmt.Errorf("%w: decode %s: %w", storage.ErrCorruptRecord, path, err)
		}
		expected, err := check(record)
		if err != nil {
			return fmt.Errorf("%w: %s: %w", storage.ErrCorruptRecord, path, err)
		}
		if expected != path {
			return fmt.Errorf("%w: record at %s belongs at %s", storage.ErrCorruptRecord, path, expected)
		}
		records[index] = record
		found[index] = true
		return nil
	}); err != nil {
		return nil, false, err
	}
	result := make([]T, 0, len(records))
	for index, record := range records {
		if found[index] {
			result = append(result, record)
		}
	}
	return result, truncated, nil
}

func sameRunPath(
	run storage.WorkflowKey,
	namespace, workflowID, runID string,
	path func() (string, error),
) (string, error) {
	if namespace != run.Namespace || workflowID != run.WorkflowID || runID != run.RunID {
		return "", fmt.Errorf("record belongs to another run")
	}
	return path()
}
