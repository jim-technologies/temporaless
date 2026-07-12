package connectstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Handler struct {
	Store      storage.Store
	ClaimStore storage.ClaimStore
}

// QueryHandler adapts a plain storage.Store into RecordQueryService for local
// and development use. It is not an indexed production query path: ordering and
// pagination are rejected, and broad activity listing walks workflow runs.
type QueryHandler struct {
	Store      storage.Store
	ClaimStore storage.ClaimStore
}

var _ temporalessv1connect.RecordStoreServiceClient = (*Handler)(nil)
var _ temporalessv1connect.RecordQueryServiceClient = (*QueryHandler)(nil)

func NewHandler(store storage.Store) *Handler {
	handler := &Handler{Store: store}
	if claimStore, ok := store.(storage.ClaimStore); ok {
		handler.ClaimStore = claimStore
	}
	return handler
}

func NewHandlerWithClaims(store storage.Store, claimStore storage.ClaimStore) *Handler {
	return &Handler{Store: store, ClaimStore: claimStore}
}

func NewQueryHandler(store storage.Store) *QueryHandler {
	handler := &QueryHandler{Store: store}
	if claimStore, ok := store.(storage.ClaimStore); ok {
		handler.ClaimStore = claimStore
	}
	return handler
}

func NewQueryHandlerWithClaims(store storage.Store, claimStore storage.ClaimStore) *QueryHandler {
	return &QueryHandler{Store: store, ClaimStore: claimStore}
}

// NewHTTPHandler mounts only RecordStoreService.
func NewHTTPHandler(store storage.Store, opts ...connect.HandlerOption) (string, http.Handler) {
	return temporalessv1connect.NewRecordStoreServiceHandler(NewHandler(store), opts...)
}

// NewHTTPHandlerWithClaims mounts only RecordStoreService with an explicit
// claim store.
func NewHTTPHandlerWithClaims(store storage.Store, claimStore storage.ClaimStore, opts ...connect.HandlerOption) (string, http.Handler) {
	return temporalessv1connect.NewRecordStoreServiceHandler(NewHandlerWithClaims(store, claimStore), opts...)
}

// NewHTTPHandlerWithQuery mounts RecordStoreService plus a caller-supplied
// RecordQueryService implementation. Use this for production indexed query
// adapters.
func NewHTTPHandlerWithQuery(store storage.Store, query temporalessv1connect.RecordQueryServiceHandler, opts ...connect.HandlerOption) (string, http.Handler) {
	return newCombinedHTTPHandler(NewHandler(store), query, opts...)
}

// NewHTTPHandlerWithClaimsAndQuery mounts RecordStoreService with an explicit
// claim store plus a caller-supplied RecordQueryService implementation.
func NewHTTPHandlerWithClaimsAndQuery(store storage.Store, claimStore storage.ClaimStore, query temporalessv1connect.RecordQueryServiceHandler, opts ...connect.HandlerOption) (string, http.Handler) {
	return newCombinedHTTPHandler(NewHandlerWithClaims(store, claimStore), query, opts...)
}

// NewHTTPHandlerWithLocalQuery mounts RecordStoreService plus the local/dev
// RecordQueryService fallback. Production search, inspectors, and exact
// retention should pass an indexed query handler to NewHTTPHandlerWithQuery
// instead.
func NewHTTPHandlerWithLocalQuery(store storage.Store, opts ...connect.HandlerOption) (string, http.Handler) {
	return NewHTTPHandlerWithQuery(store, NewQueryHandler(store), opts...)
}

// NewHTTPHandlerWithClaimsAndLocalQuery mounts RecordStoreService with an
// explicit claim store plus the local/dev RecordQueryService fallback.
func NewHTTPHandlerWithClaimsAndLocalQuery(store storage.Store, claimStore storage.ClaimStore, opts ...connect.HandlerOption) (string, http.Handler) {
	return NewHTTPHandlerWithClaimsAndQuery(store, claimStore, NewQueryHandlerWithClaims(store, claimStore), opts...)
}

func newCombinedHTTPHandler(store temporalessv1connect.RecordStoreServiceHandler, query temporalessv1connect.RecordQueryServiceHandler, opts ...connect.HandlerOption) (string, http.Handler) {
	storePath, storeHandler := temporalessv1connect.NewRecordStoreServiceHandler(store, opts...)
	queryPath, queryHandler := temporalessv1connect.NewRecordQueryServiceHandler(query, opts...)
	mux := http.NewServeMux()
	mux.Handle(storePath, storeHandler)
	mux.Handle(queryPath, queryHandler)
	return "/", mux
}

func (handler *Handler) GetStoreCapabilities(ctx context.Context, _ *connect.Request[temporalessv1.GetStoreCapabilitiesRequest]) (*connect.Response[temporalessv1.GetStoreCapabilitiesResponse], error) {
	capability := storage.NoClaims
	if handler.ClaimStore != nil {
		var err error
		capability, err = handler.ClaimStore.ClaimCapability(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(&temporalessv1.GetStoreCapabilitiesResponse{
		ClaimCapability: capability,
	}), nil
}

func (handler *Handler) GetWorkflow(ctx context.Context, req *connect.Request[temporalessv1.GetWorkflowRequest]) (*connect.Response[temporalessv1.GetWorkflowResponse], error) {
	record, found, err := handler.Store.GetWorkflow(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.GetWorkflowResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutWorkflow(ctx context.Context, req *connect.Request[temporalessv1.PutWorkflowRequest]) (*connect.Response[temporalessv1.PutWorkflowResponse], error) {
	if err := handler.Store.PutWorkflow(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutWorkflowResponse{}), nil
}

func (handler *Handler) GetLatestWorkflowRun(ctx context.Context, req *connect.Request[temporalessv1.GetLatestWorkflowRunRequest]) (*connect.Response[temporalessv1.GetLatestWorkflowRunResponse], error) {
	if req.Msg.GetWorkflowId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("workflow_id is required"))
	}
	records, err := handler.Store.ListWorkflows(ctx, req.Msg.GetNamespace(), req.Msg.GetWorkflowId(), temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var latest *temporalessv1.WorkflowRecord
	var latestRecordTime *timestamppb.Timestamp
	for _, record := range records {
		recordTime := workflowPointerRecordTime(record)
		if latest == nil {
			latest = record
			latestRecordTime = recordTime
			continue
		}
		if recordTime != nil && (latestRecordTime == nil || recordTime.AsTime().After(latestRecordTime.AsTime())) {
			latest = record
			latestRecordTime = recordTime
		}
	}
	if latest == nil {
		return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{}), nil
	}
	return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{
		Found: true,
		Pointer: &temporalessv1.LatestWorkflowRunPointer{
			Key:        latest.GetKey(),
			Status:     latest.GetStatus(),
			RecordTime: latestRecordTime,
			UpdatedAt:  timestamppb.Now(),
		},
	}), nil
}

func (handler *Handler) GetTimer(ctx context.Context, req *connect.Request[temporalessv1.GetTimerRequest]) (*connect.Response[temporalessv1.GetTimerResponse], error) {
	record, found, err := handler.Store.GetTimer(ctx, storage.TimerKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.GetTimerResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutTimer(ctx context.Context, req *connect.Request[temporalessv1.PutTimerRequest]) (*connect.Response[temporalessv1.PutTimerResponse], error) {
	if err := handler.Store.PutTimer(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutTimerResponse{}), nil
}

func (handler *Handler) GetActivity(ctx context.Context, req *connect.Request[temporalessv1.GetActivityRequest]) (*connect.Response[temporalessv1.GetActivityResponse], error) {
	record, found, err := handler.Store.GetActivity(ctx, storage.ActivityKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.GetActivityResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutActivity(ctx context.Context, req *connect.Request[temporalessv1.PutActivityRequest]) (*connect.Response[temporalessv1.PutActivityResponse], error) {
	if err := handler.Store.PutActivity(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutActivityResponse{}), nil
}

func (handler *Handler) GetEvent(ctx context.Context, req *connect.Request[temporalessv1.GetEventRequest]) (*connect.Response[temporalessv1.GetEventResponse], error) {
	record, found, err := handler.Store.GetEvent(ctx, storage.EventKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.GetEventResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutEvent(ctx context.Context, req *connect.Request[temporalessv1.PutEventRequest]) (*connect.Response[temporalessv1.PutEventResponse], error) {
	if err := handler.Store.PutEvent(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutEventResponse{}), nil
}

func (handler *Handler) ListWorkflows(ctx context.Context, req *connect.Request[temporalessv1.ListWorkflowsRequest]) (*connect.Response[temporalessv1.ListWorkflowsResponse], error) {
	records, err := handler.Store.ListWorkflows(ctx, req.Msg.GetNamespace(), req.Msg.GetWorkflowId(), req.Msg.GetStatus())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.ListWorkflowsResponse{Records: records}), nil
}

func (handler *Handler) ListActivities(ctx context.Context, req *connect.Request[temporalessv1.ListActivitiesRequest]) (*connect.Response[temporalessv1.ListActivitiesResponse], error) {
	records, err := handler.Store.ListActivities(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.ListActivitiesResponse{Records: records}), nil
}

func (handler *Handler) ListTimers(ctx context.Context, req *connect.Request[temporalessv1.ListTimersRequest]) (*connect.Response[temporalessv1.ListTimersResponse], error) {
	records, err := handler.Store.ListTimers(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()), req.Msg.GetStatus())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.ListTimersResponse{Records: records}), nil
}

func (handler *Handler) ListEvents(ctx context.Context, req *connect.Request[temporalessv1.ListEventsRequest]) (*connect.Response[temporalessv1.ListEventsResponse], error) {
	records, err := handler.Store.ListEvents(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.ListEventsResponse{Records: records}), nil
}

func (handler *Handler) ListClaims(ctx context.Context, req *connect.Request[temporalessv1.ListClaimsRequest]) (*connect.Response[temporalessv1.ListClaimsResponse], error) {
	records, err := handler.listClaimsForRun(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&temporalessv1.ListClaimsResponse{Records: records}), nil
}

func (handler *Handler) DeleteWorkflow(ctx context.Context, req *connect.Request[temporalessv1.DeleteWorkflowRequest]) (*connect.Response[temporalessv1.DeleteWorkflowResponse], error) {
	deleted, err := handler.Store.DeleteWorkflow(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteWorkflowResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteActivity(ctx context.Context, req *connect.Request[temporalessv1.DeleteActivityRequest]) (*connect.Response[temporalessv1.DeleteActivityResponse], error) {
	deleted, err := handler.Store.DeleteActivity(ctx, storage.ActivityKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteActivityResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteTimer(ctx context.Context, req *connect.Request[temporalessv1.DeleteTimerRequest]) (*connect.Response[temporalessv1.DeleteTimerResponse], error) {
	deleted, err := handler.Store.DeleteTimer(ctx, storage.TimerKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteTimerResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteEvent(ctx context.Context, req *connect.Request[temporalessv1.DeleteEventRequest]) (*connect.Response[temporalessv1.DeleteEventResponse], error) {
	deleted, err := handler.Store.DeleteEvent(ctx, storage.EventKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteEventResponse{Deleted: deleted}), nil
}

// DeleteRun removes one externally quiesced workflow run. It is a bounded,
// multi-step cleanup operation, not a transaction or execution fence: callers
// must ensure no worker can create claims or write records for the run while
// deletion is in progress.
func (handler *Handler) DeleteRun(ctx context.Context, req *connect.Request[temporalessv1.DeleteRunRequest]) (*connect.Response[temporalessv1.DeleteRunResponse], error) {
	key := storage.WorkflowKeyFromProto(req.Msg.GetKey())
	var deleted uint32

	// Claims may live in a separately configured store. Snapshot every record
	// kind and validate every embedded key before the first mutation: a corrupt
	// payload under this run's storage prefix must never redirect deletion to a
	// different run.
	claims, err := handler.listClaimsForRun(ctx, key)
	if err != nil {
		return nil, err
	}
	activities, err := handler.Store.ListActivities(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	timers, err := handler.Store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	events, err := handler.Store.ListEvents(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	for _, activity := range activities {
		activityKey := storage.ActivityKeyFromProto(activity.GetKey())
		if err := validateListedRunRecordKey(
			"activity",
			key,
			activityKey.Namespace,
			activityKey.WorkflowID,
			activityKey.RunID,
			activityKey.Validate(),
		); err != nil {
			return nil, err
		}
	}
	for _, timer := range timers {
		timerKey := storage.TimerKeyFromProto(timer.GetKey())
		if err := validateListedRunRecordKey(
			"timer",
			key,
			timerKey.Namespace,
			timerKey.WorkflowID,
			timerKey.RunID,
			timerKey.Validate(),
		); err != nil {
			return nil, err
		}
	}
	for _, event := range events {
		eventKey := storage.EventKeyFromProto(event.GetKey())
		if err := validateListedRunRecordKey(
			"event",
			key,
			eventKey.Namespace,
			eventKey.WorkflowID,
			eventKey.RunID,
			eventKey.Validate(),
		); err != nil {
			return nil, err
		}
	}

	// The caller has externally quiesced the run and every snapshot is valid.
	// Claims are removed first so no stale run-scoped claim survives successful
	// cleanup. This ordering does not make DeleteRun a fence or a transaction.
	if handler.ClaimStore != nil {
		for _, claim := range claims {
			ok, err := handler.ClaimStore.DeleteClaim(ctx, storage.ClaimKeyFromProto(claim.GetKey()))
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			if ok {
				deleted++
			}
		}
	}
	for _, activity := range activities {
		ok, err := handler.Store.DeleteActivity(ctx, storage.ActivityKeyFromProto(activity.GetKey()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if ok {
			deleted++
		}
	}
	for _, timer := range timers {
		ok, err := handler.Store.DeleteTimer(ctx, storage.TimerKeyFromProto(timer.GetKey()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if ok {
			deleted++
		}
	}
	for _, event := range events {
		ok, err := handler.Store.DeleteEvent(ctx, storage.EventKeyFromProto(event.GetKey()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if ok {
			deleted++
		}
	}

	ok, err := handler.Store.DeleteWorkflow(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if ok {
		deleted++
	}
	return connect.NewResponse(&temporalessv1.DeleteRunResponse{Deleted: deleted}), nil
}

func validateListedRunRecordKey(
	recordKind string,
	target storage.WorkflowKey,
	recordNamespace string,
	recordWorkflowID string,
	recordRunID string,
	validateErr error,
) error {
	if validateErr != nil {
		return connect.NewError(
			connect.CodeDataLoss,
			fmt.Errorf("invalid %s key in run listing: %w", recordKind, validateErr),
		)
	}
	targetNamespace := target.Namespace
	if targetNamespace == "" {
		targetNamespace = storage.DefaultNamespace
	}
	if recordNamespace == "" {
		recordNamespace = storage.DefaultNamespace
	}
	if recordNamespace != targetNamespace || recordWorkflowID != target.WorkflowID || recordRunID != target.RunID {
		return connect.NewError(
			connect.CodeDataLoss,
			fmt.Errorf("%s payload key does not match requested workflow run", recordKind),
		)
	}
	return nil
}

func (handler *Handler) listClaimsForRun(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error) {
	if handler.ClaimStore == nil {
		return nil, nil
	}
	capability, err := handler.ClaimStore.ClaimCapability(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if capability != storage.CreateOnlyClaims && capability != storage.CASClaims {
		return nil, nil
	}
	claimRunStore, ok := handler.ClaimStore.(storage.ClaimRunStore)
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store does not support run-scoped claim listing"))
	}
	records, err := claimRunStore.ListClaims(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// Validate the complete snapshot before DeleteRun removes any claim. A
	// corrupt payload under this prefix must never redirect bounded deletion to
	// another run merely because its embedded key says so.
	for _, record := range records {
		claimKey := storage.ClaimKeyFromProto(record.GetKey())
		if err := validateListedRunRecordKey(
			"claim",
			key,
			claimKey.Namespace,
			claimKey.WorkflowID,
			claimKey.RunID,
			claimKey.Validate(),
		); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func (handler *Handler) GetClaim(ctx context.Context, req *connect.Request[temporalessv1.GetClaimRequest]) (*connect.Response[temporalessv1.GetClaimResponse], error) {
	if handler.ClaimStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store is required"))
	}

	record, found, err := handler.ClaimStore.GetClaim(ctx, storage.ClaimKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.GetClaimResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) TryCreateClaim(ctx context.Context, req *connect.Request[temporalessv1.TryCreateClaimRequest]) (*connect.Response[temporalessv1.TryCreateClaimResponse], error) {
	if handler.ClaimStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store is required"))
	}

	created, err := handler.ClaimStore.TryCreateClaim(ctx, req.Msg.GetRecord())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.TryCreateClaimResponse{
		Created: created,
	}), nil
}

func (handler *Handler) DeleteClaim(ctx context.Context, req *connect.Request[temporalessv1.DeleteClaimRequest]) (*connect.Response[temporalessv1.DeleteClaimResponse], error) {
	if handler.ClaimStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store is required"))
	}

	key := storage.ClaimKeyFromProto(req.Msg.GetKey())
	deleted, err := handler.ClaimStore.DeleteClaim(ctx, key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.DeleteClaimResponse{
		Deleted: deleted,
	}), nil
}

func (handler *Handler) Sweep(ctx context.Context, req *connect.Request[temporalessv1.SweepRequest]) (*connect.Response[temporalessv1.SweepResponse], error) {
	deleted, err := janitor.Sweep(ctx, handler.Store, handler.ClaimStore, req.Msg)
	if err != nil {
		return nil, sweepConnectError(err)
	}
	return connect.NewResponse(&temporalessv1.SweepResponse{Deleted: deleted}), nil
}

func (handler *Handler) DueTimers(ctx context.Context, req *connect.Request[temporalessv1.DueTimersRequest]) (*connect.Response[temporalessv1.DueTimersResponse], error) {
	due, err := handler.Store.DueTimers(ctx, req.Msg.GetNamespace(), req.Msg.GetNow().AsTime())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &temporalessv1.DueTimersResponse{
		Due: make([]*temporalessv1.DueTimer, 0, len(due)),
	}
	for _, entry := range due {
		resp.Due = append(resp.Due, &temporalessv1.DueTimer{
			Key:      entry.Key.Proto(),
			Record:   entry.Record,
			Workflow: entry.Workflow,
		})
	}
	return connect.NewResponse(resp), nil
}

func workflowPointerRecordTime(record *temporalessv1.WorkflowRecord) *timestamppb.Timestamp {
	if record.GetCompletedAt() != nil {
		return record.GetCompletedAt()
	}
	return record.GetCreatedAt()
}

func (handler *QueryHandler) ListWorkflows(ctx context.Context, req *connect.Request[temporalessv1.ListWorkflowsRequest]) (*connect.Response[temporalessv1.ListWorkflowsResponse], error) {
	if err := rejectUnsupportedQueryOptions(req.Msg.GetOrderBy(), req.Msg.GetPageSize(), req.Msg.GetPageToken()); err != nil {
		return nil, err
	}
	records, err := handler.Store.ListWorkflows(ctx, req.Msg.GetNamespace(), req.Msg.GetWorkflowId(), req.Msg.GetStatus())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.ListWorkflowsResponse{Records: records}), nil
}

func (handler *QueryHandler) ListActivities(ctx context.Context, req *connect.Request[temporalessv1.RecordQueryServiceListActivitiesRequest]) (*connect.Response[temporalessv1.RecordQueryServiceListActivitiesResponse], error) {
	if err := rejectUnsupportedQueryOptions(req.Msg.GetOrderBy(), req.Msg.GetPageSize(), req.Msg.GetPageToken()); err != nil {
		return nil, err
	}
	workflowID := req.Msg.GetWorkflowId()
	runID := req.Msg.GetRunId()
	status := req.Msg.GetStatus()

	var records []*temporalessv1.ActivityRecord
	if runID != "" {
		if workflowID == "" {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("workflow_id is required when run_id is set"))
		}
		activities, err := handler.Store.ListActivities(ctx, storage.WorkflowKey{
			Namespace:  req.Msg.GetNamespace(),
			WorkflowID: workflowID,
			RunID:      runID,
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, activity := range activities {
			if status == temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED || activity.GetStatus() == status {
				records = append(records, activity)
			}
		}
		return connect.NewResponse(&temporalessv1.RecordQueryServiceListActivitiesResponse{Records: records}), nil
	}

	workflows, err := handler.Store.ListWorkflows(ctx, req.Msg.GetNamespace(), workflowID, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	for _, workflow := range workflows {
		activities, err := handler.Store.ListActivities(ctx, storage.WorkflowKeyFromProto(workflow.GetKey()))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		for _, activity := range activities {
			if status == temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED || activity.GetStatus() == status {
				records = append(records, activity)
			}
		}
	}
	return connect.NewResponse(&temporalessv1.RecordQueryServiceListActivitiesResponse{Records: records}), nil
}

func (handler *QueryHandler) Sweep(ctx context.Context, req *connect.Request[temporalessv1.SweepRequest]) (*connect.Response[temporalessv1.SweepResponse], error) {
	deleted, err := janitor.Sweep(ctx, handler.Store, handler.ClaimStore, req.Msg)
	if err != nil {
		return nil, sweepConnectError(err)
	}
	return connect.NewResponse(&temporalessv1.SweepResponse{Deleted: deleted}), nil
}

func sweepConnectError(err error) error {
	switch {
	case errors.Is(err, janitor.ErrClaimRunListingUnsupported):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, janitor.ErrRunListingDataLoss):
		return connect.NewError(connect.CodeDataLoss, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func (handler *QueryHandler) DueTimers(ctx context.Context, req *connect.Request[temporalessv1.RecordQueryServiceDueTimersRequest]) (*connect.Response[temporalessv1.RecordQueryServiceDueTimersResponse], error) {
	due, err := handler.Store.DueTimers(ctx, req.Msg.GetNamespace(), req.Msg.GetNow().AsTime())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	resp := &temporalessv1.RecordQueryServiceDueTimersResponse{
		Due: make([]*temporalessv1.DueTimer, 0, len(due)),
	}
	for _, entry := range due {
		resp.Due = append(resp.Due, &temporalessv1.DueTimer{
			Key:      entry.Key.Proto(),
			Record:   entry.Record,
			Workflow: entry.Workflow,
		})
	}
	return connect.NewResponse(resp), nil
}

func rejectUnsupportedQueryOptions(orderBy string, pageSize int32, pageToken string) error {
	if orderBy != "" || pageSize != 0 || pageToken != "" {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("local storage query handler does not support order_by or pagination"))
	}
	return nil
}
