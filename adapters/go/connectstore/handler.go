package connectstore

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"buf.build/go/protovalidate"
	"connectrpc.com/connect"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

type Handler struct {
	Store              storage.Store
	ClaimStore         storage.ClaimStore
	EventDeliveryStore storage.EventDeliveryStore
}

// QueryHandler adapts an explicit storage.QueryStore into RecordQueryService.
// Core point stores do not implement cross-run queries.
type QueryHandler struct {
	Query storage.QueryStore
}

var _ temporalessv1connect.RecordStoreServiceClient = (*Handler)(nil)
var _ temporalessv1connect.RecordQueryServiceClient = (*QueryHandler)(nil)

func NewHandler(store storage.Store) *Handler {
	handler := &Handler{Store: store}
	if claimStore, ok := store.(storage.ClaimStore); ok {
		handler.ClaimStore = claimStore
	}
	if eventStore, ok := store.(storage.EventDeliveryStore); ok {
		handler.EventDeliveryStore = eventStore
	}
	return handler
}

func NewHandlerWithClaims(store storage.Store, claimStore storage.ClaimStore) *Handler {
	handler := &Handler{Store: store, ClaimStore: claimStore}
	if eventStore, ok := store.(storage.EventDeliveryStore); ok {
		handler.EventDeliveryStore = eventStore
	}
	return handler
}

func NewQueryHandler(query storage.QueryStore) *QueryHandler {
	return &QueryHandler{Query: query}
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

// NewHTTPHandlerWithLocalQuery mounts RecordStoreService plus an explicit
// local QueryStore such as the offline/development scanquery adapter.
func NewHTTPHandlerWithLocalQuery(store storage.Store, query storage.QueryStore, opts ...connect.HandlerOption) (string, http.Handler) {
	return NewHTTPHandlerWithQuery(store, NewQueryHandler(query), opts...)
}

// NewHTTPHandlerWithClaimsAndLocalQuery mounts RecordStoreService with an
// explicit claim store plus an explicit local QueryStore.
func NewHTTPHandlerWithClaimsAndLocalQuery(store storage.Store, claimStore storage.ClaimStore, query storage.QueryStore, opts ...connect.HandlerOption) (string, http.Handler) {
	return NewHTTPHandlerWithClaimsAndQuery(store, claimStore, NewQueryHandler(query), opts...)
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
		capability, err = currentClaimCapability(capability)
		if err != nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, err)
		}
	}
	eventCapability := storage.NoAtomicEventDelivery
	if handler.EventDeliveryStore != nil {
		var err error
		eventCapability, err = handler.EventDeliveryStore.EventDeliveryCapability(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		switch eventCapability {
		case temporalessv1.EventDeliveryCapability_EVENT_DELIVERY_CAPABILITY_UNSPECIFIED:
			eventCapability = storage.NoAtomicEventDelivery
		case storage.NoAtomicEventDelivery, storage.CreateOnlyEventDelivery:
		default:
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf(
					"event delivery store returned invalid capability %s",
					eventCapability,
				),
			)
		}
	}
	return connect.NewResponse(&temporalessv1.GetStoreCapabilitiesResponse{
		ClaimCapability:         capability,
		EventDeliveryCapability: eventCapability,
	}), nil
}

func (handler *Handler) GetWorkflow(ctx context.Context, req *connect.Request[temporalessv1.GetWorkflowRequest]) (*connect.Response[temporalessv1.GetWorkflowResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.WorkflowKeyFromProto(req.Msg.GetKey())
	record, found, err := handler.Store.GetWorkflow(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if err := validateFoundPayload("workflow", found, record != nil); err != nil {
		return nil, dataLossError(err)
	}
	if found {
		if err := storage.ValidateWorkflowRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
	}

	return connect.NewResponse(&temporalessv1.GetWorkflowResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutWorkflow(ctx context.Context, req *connect.Request[temporalessv1.PutWorkflowRequest]) (*connect.Response[temporalessv1.PutWorkflowResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := storage.ValidateWorkflowRecord(req.Msg.GetRecord(), storage.WorkflowKeyFromProto(req.Msg.GetRecord().GetKey())); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := handler.Store.PutWorkflow(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutWorkflowResponse{}), nil
}

func (handler *Handler) GetLatestWorkflowRun(ctx context.Context, req *connect.Request[temporalessv1.GetLatestWorkflowRunRequest]) (*connect.Response[temporalessv1.GetLatestWorkflowRunResponse], error) {
	if req.Msg.GetWorkflowId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("workflow_id is required"))
	}
	requested := storage.WorkflowKey{
		Namespace:  req.Msg.GetNamespace(),
		WorkflowID: req.Msg.GetWorkflowId(),
		RunID:      "validation",
	}
	if err := requested.Validate(); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	pointer, found, err := handler.Store.GetLatestWorkflowRun(
		ctx,
		req.Msg.GetNamespace(),
		req.Msg.GetWorkflowId(),
	)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if !found {
		if pointer != nil {
			return nil, dataLossError(fmt.Errorf("latest workflow pointer returned found=false with a payload"))
		}
		return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{}), nil
	}
	if err := storage.ValidateLatestWorkflowRunPointer(pointer, req.Msg.GetNamespace(), req.Msg.GetWorkflowId()); err != nil {
		return nil, dataLossError(err)
	}
	referencedKey := storage.WorkflowKeyFromProto(pointer.GetKey())
	workflow, workflowFound, err := handler.Store.GetWorkflow(ctx, referencedKey)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if !workflowFound {
		return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{}), nil
	}
	if err := storage.ValidateLatestWorkflowRunReference(pointer, workflow); err != nil {
		if errors.Is(err, storage.ErrStaleLatestPointer) {
			return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{}), nil
		}
		return nil, dataLossError(err)
	}
	return connect.NewResponse(&temporalessv1.GetLatestWorkflowRunResponse{
		Found:   true,
		Pointer: pointer,
	}), nil
}

func (handler *Handler) GetTimer(ctx context.Context, req *connect.Request[temporalessv1.GetTimerRequest]) (*connect.Response[temporalessv1.GetTimerResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.TimerKeyFromProto(req.Msg.GetKey())
	record, found, err := handler.Store.GetTimer(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if err := validateFoundPayload("timer", found, record != nil); err != nil {
		return nil, dataLossError(err)
	}
	if found {
		if err := storage.ValidateTimerRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
	}

	return connect.NewResponse(&temporalessv1.GetTimerResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutTimer(ctx context.Context, req *connect.Request[temporalessv1.PutTimerRequest]) (*connect.Response[temporalessv1.PutTimerResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := storage.ValidateTimerRecord(req.Msg.GetRecord(), storage.TimerKeyFromProto(req.Msg.GetRecord().GetKey())); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := handler.Store.PutTimer(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutTimerResponse{}), nil
}

func (handler *Handler) GetActivity(ctx context.Context, req *connect.Request[temporalessv1.GetActivityRequest]) (*connect.Response[temporalessv1.GetActivityResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.ActivityKeyFromProto(req.Msg.GetKey())
	record, found, err := handler.Store.GetActivity(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if err := validateFoundPayload("activity", found, record != nil); err != nil {
		return nil, dataLossError(err)
	}
	if found {
		if err := storage.ValidateActivityRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
	}

	return connect.NewResponse(&temporalessv1.GetActivityResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutActivity(ctx context.Context, req *connect.Request[temporalessv1.PutActivityRequest]) (*connect.Response[temporalessv1.PutActivityResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := storage.ValidateActivityRecord(req.Msg.GetRecord(), storage.ActivityKeyFromProto(req.Msg.GetRecord().GetKey())); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := handler.Store.PutActivity(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutActivityResponse{}), nil
}

func (handler *Handler) GetEvent(ctx context.Context, req *connect.Request[temporalessv1.GetEventRequest]) (*connect.Response[temporalessv1.GetEventResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.EventKeyFromProto(req.Msg.GetKey())
	record, found, err := handler.Store.GetEvent(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if err := validateFoundPayload("event", found, record != nil); err != nil {
		return nil, dataLossError(err)
	}
	if found {
		if err := storage.ValidateEventRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
	}

	return connect.NewResponse(&temporalessv1.GetEventResponse{
		Found:  found,
		Record: record,
	}), nil
}

func (handler *Handler) PutEvent(ctx context.Context, req *connect.Request[temporalessv1.PutEventRequest]) (*connect.Response[temporalessv1.PutEventResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	if err := storage.ValidateEventRecord(req.Msg.GetRecord(), storage.EventKeyFromProto(req.Msg.GetRecord().GetKey())); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := handler.Store.PutEvent(ctx, req.Msg.GetRecord()); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.PutEventResponse{}), nil
}

func (handler *Handler) DeliverEvent(
	ctx context.Context,
	req *connect.Request[temporalessv1.DeliverEventRequest],
) (*connect.Response[temporalessv1.DeliverEventResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.EventKeyFromProto(req.Msg.GetRecord().GetKey())
	if err := storage.ValidateEventDeliveryRecord(req.Msg.GetRecord(), key); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if handler.EventDeliveryStore == nil {
		return nil, eventDeliveryConnectError(
			storage.ErrEventDeliveryUnsupported,
			key,
		)
	}
	capability, err := handler.EventDeliveryStore.EventDeliveryCapability(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if capability == temporalessv1.EventDeliveryCapability_EVENT_DELIVERY_CAPABILITY_UNSPECIFIED ||
		capability == storage.NoAtomicEventDelivery {
		return nil, eventDeliveryConnectError(
			storage.ErrEventDeliveryUnsupported,
			key,
		)
	}
	if capability != storage.CreateOnlyEventDelivery {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("event delivery store returned invalid capability %s", capability),
		)
	}
	disposition, err := handler.EventDeliveryStore.DeliverEvent(ctx, req.Msg.GetRecord())
	if err != nil {
		return nil, eventDeliveryConnectError(err, key)
	}
	switch disposition {
	case temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED,
		temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_IDEMPOTENT:
	default:
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("event delivery store returned invalid disposition %s", disposition),
		)
	}
	return connect.NewResponse(&temporalessv1.DeliverEventResponse{
		Disposition: disposition,
	}), nil
}

func (handler *Handler) ListActivities(ctx context.Context, req *connect.Request[temporalessv1.ListActivitiesRequest]) (*connect.Response[temporalessv1.ListActivitiesResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.WorkflowKeyFromProto(req.Msg.GetKey())
	records, err := handler.Store.ListActivities(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	for _, record := range records {
		recordKey := storage.ActivityKeyFromProto(record.GetKey())
		if err := storage.ValidateActivityRecord(record, recordKey); err != nil {
			return nil, dataLossError(err)
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, dataLossError(fmt.Errorf("activity list payload crosses the requested workflow run"))
		}
	}
	return connect.NewResponse(&temporalessv1.ListActivitiesResponse{Records: records}), nil
}

func (handler *Handler) ListTimers(ctx context.Context, req *connect.Request[temporalessv1.ListTimersRequest]) (*connect.Response[temporalessv1.ListTimersResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.WorkflowKeyFromProto(req.Msg.GetKey())
	records, err := handler.Store.ListTimers(ctx, key, req.Msg.GetStatus())
	if err != nil {
		return nil, storeConnectError(err)
	}
	for _, record := range records {
		recordKey := storage.TimerKeyFromProto(record.GetKey())
		if err := storage.ValidateTimerRecord(record, recordKey); err != nil {
			return nil, dataLossError(err)
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, dataLossError(fmt.Errorf("timer list payload crosses the requested workflow run"))
		}
		if req.Msg.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED && record.GetStatus() != req.Msg.GetStatus() {
			return nil, dataLossError(fmt.Errorf("timer list payload does not match the requested status"))
		}
	}
	return connect.NewResponse(&temporalessv1.ListTimersResponse{Records: records}), nil
}

func (handler *Handler) ListEvents(ctx context.Context, req *connect.Request[temporalessv1.ListEventsRequest]) (*connect.Response[temporalessv1.ListEventsResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	key := storage.WorkflowKeyFromProto(req.Msg.GetKey())
	records, err := handler.Store.ListEvents(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	for _, record := range records {
		recordKey := storage.EventKeyFromProto(record.GetKey())
		if err := storage.ValidateEventRecord(record, recordKey); err != nil {
			return nil, dataLossError(err)
		}
		if !sameWorkflowRun(key, recordKey.Namespace, recordKey.WorkflowID, recordKey.RunID) {
			return nil, dataLossError(fmt.Errorf("event list payload crosses the requested workflow run"))
		}
	}
	return connect.NewResponse(&temporalessv1.ListEventsResponse{Records: records}), nil
}

func (handler *Handler) ListClaims(ctx context.Context, req *connect.Request[temporalessv1.ListClaimsRequest]) (*connect.Response[temporalessv1.ListClaimsResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	records, err := handler.listClaimsForRun(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&temporalessv1.ListClaimsResponse{Records: records}), nil
}

func (handler *Handler) DeleteWorkflow(ctx context.Context, req *connect.Request[temporalessv1.DeleteWorkflowRequest]) (*connect.Response[temporalessv1.DeleteWorkflowResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	deleted, err := handler.Store.DeleteWorkflow(ctx, storage.WorkflowKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteWorkflowResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteActivity(ctx context.Context, req *connect.Request[temporalessv1.DeleteActivityRequest]) (*connect.Response[temporalessv1.DeleteActivityResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	deleted, err := handler.Store.DeleteActivity(ctx, storage.ActivityKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&temporalessv1.DeleteActivityResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteTimer(ctx context.Context, req *connect.Request[temporalessv1.DeleteTimerRequest]) (*connect.Response[temporalessv1.DeleteTimerResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	deleted, err := handler.Store.DeleteTimer(ctx, storage.TimerKeyFromProto(req.Msg.GetKey()))
	if err != nil {
		return nil, storeConnectError(err)
	}
	return connect.NewResponse(&temporalessv1.DeleteTimerResponse{Deleted: deleted}), nil
}

func (handler *Handler) DeleteEvent(ctx context.Context, req *connect.Request[temporalessv1.DeleteEventRequest]) (*connect.Response[temporalessv1.DeleteEventResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
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
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
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
		return nil, storeConnectError(err)
	}
	timers, err := handler.Store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil {
		return nil, storeConnectError(err)
	}
	events, err := handler.Store.ListEvents(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}

	for _, activity := range activities {
		activityKey := storage.ActivityKeyFromProto(activity.GetKey())
		if err := storage.ValidateActivityRecord(activity, activityKey); err != nil {
			return nil, dataLossError(err)
		}
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
		if err := storage.ValidateTimerRecord(timer, timerKey); err != nil {
			return nil, dataLossError(err)
		}
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
		if err := storage.ValidateEventRecord(event, eventKey); err != nil {
			return nil, dataLossError(err)
		}
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
	capability, err = currentClaimCapability(capability)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if capability != storage.CreateOnlyClaims {
		return nil, nil
	}
	claimRunStore, ok := handler.ClaimStore.(storage.ClaimRunStore)
	if !ok {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store does not support run-scoped claim listing"))
	}
	records, err := claimRunStore.ListClaims(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}

	// Validate the complete snapshot before DeleteRun removes any claim. A
	// corrupt payload under this prefix must never redirect bounded deletion to
	// another run merely because its embedded key says so.
	for _, record := range records {
		claimKey := storage.ClaimKeyFromProto(record.GetKey())
		if err := storage.ValidateClaimRecord(record, claimKey); err != nil {
			return nil, dataLossError(err)
		}
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
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}

	key := storage.ClaimKeyFromProto(req.Msg.GetKey())
	record, found, err := handler.ClaimStore.GetClaim(ctx, key)
	if err != nil {
		return nil, storeConnectError(err)
	}
	if err := validateFoundPayload("claim", found, record != nil); err != nil {
		return nil, dataLossError(err)
	}
	if found {
		if err := storage.ValidateClaimRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
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
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	capability, err := handler.ClaimStore.ClaimCapability(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	capability, err = currentClaimCapability(capability)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if capability != storage.CreateOnlyClaims {
		return nil, connect.NewError(
			connect.CodeFailedPrecondition,
			fmt.Errorf("claim store does not support atomic create"),
		)
	}
	if err := storage.ValidateClaimRecord(req.Msg.GetRecord(), storage.ClaimKeyFromProto(req.Msg.GetRecord().GetKey())); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	created, err := handler.ClaimStore.TryCreateClaim(ctx, req.Msg.GetRecord())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&temporalessv1.TryCreateClaimResponse{
		Created: created,
	}), nil
}

func currentClaimCapability(
	capability storage.ClaimCapability,
) (storage.ClaimCapability, error) {
	switch capability {
	case temporalessv1.ClaimCapability_CLAIM_CAPABILITY_UNSPECIFIED,
		storage.NoClaims:
		return storage.NoClaims, nil
	case storage.CreateOnlyClaims:
		return storage.CreateOnlyClaims, nil
	default:
		return storage.NoClaims, fmt.Errorf(
			"claim capability %s is unsupported by the current create-only claim interface",
			capability,
		)
	}
}

func (handler *Handler) DeleteClaim(ctx context.Context, req *connect.Request[temporalessv1.DeleteClaimRequest]) (*connect.Response[temporalessv1.DeleteClaimResponse], error) {
	if handler.ClaimStore == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("claim store is required"))
	}
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	capability, err := handler.ClaimStore.ClaimCapability(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	capability, err = currentClaimCapability(capability)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if capability != storage.CreateOnlyClaims {
		return nil, connect.NewError(
			connect.CodeFailedPrecondition,
			fmt.Errorf("claim store does not support claim release"),
		)
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

func (handler *Handler) DueTimers(ctx context.Context, req *connect.Request[temporalessv1.DueTimersRequest]) (*connect.Response[temporalessv1.DueTimersResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	now := req.Msg.GetNow().AsTime()
	due, err := handler.Store.DueTimers(ctx, req.Msg.GetNamespace(), now)
	if err != nil {
		return nil, storeConnectError(err)
	}
	resp := &temporalessv1.DueTimersResponse{
		Due: make([]*temporalessv1.DueTimer, 0, len(due)),
	}
	for _, entry := range due {
		if err := storage.ValidateDueTimer(entry, req.Msg.GetNamespace(), now); err != nil {
			return nil, dataLossError(err)
		}
		resp.Due = append(resp.Due, &temporalessv1.DueTimer{
			Key:      entry.Key.Proto(),
			Record:   entry.Record,
			Workflow: entry.Workflow,
		})
	}
	return connect.NewResponse(resp), nil
}

func (handler *QueryHandler) ListWorkflows(ctx context.Context, req *connect.Request[temporalessv1.ListWorkflowsRequest]) (*connect.Response[temporalessv1.ListWorkflowsResponse], error) {
	response, err := handler.Query.ListWorkflows(ctx, req.Msg)
	if err != nil {
		return nil, queryConnectError(err)
	}
	for _, record := range response.GetRecords() {
		key := storage.WorkflowKeyFromProto(record.GetKey())
		if err := storage.ValidateWorkflowRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
		if req.Msg.GetNamespace() != "" && namespaceOrDefault(key.Namespace) != req.Msg.GetNamespace() {
			return nil, dataLossError(fmt.Errorf("workflow query payload crosses the requested namespace"))
		}
		if req.Msg.GetWorkflowId() != "" && key.WorkflowID != req.Msg.GetWorkflowId() {
			return nil, dataLossError(fmt.Errorf("workflow query payload crosses the requested workflow_id"))
		}
		if req.Msg.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && record.GetStatus() != req.Msg.GetStatus() {
			return nil, dataLossError(fmt.Errorf("workflow query payload does not match the requested status"))
		}
	}
	return connect.NewResponse(response), nil
}

func (handler *QueryHandler) ListActivities(ctx context.Context, req *connect.Request[temporalessv1.RecordQueryServiceListActivitiesRequest]) (*connect.Response[temporalessv1.RecordQueryServiceListActivitiesResponse], error) {
	response, err := handler.Query.ListActivitiesQuery(ctx, req.Msg)
	if err != nil {
		return nil, queryConnectError(err)
	}
	for _, record := range response.GetRecords() {
		key := storage.ActivityKeyFromProto(record.GetKey())
		if err := storage.ValidateActivityRecord(record, key); err != nil {
			return nil, dataLossError(err)
		}
		if req.Msg.GetNamespace() != "" && namespaceOrDefault(key.Namespace) != req.Msg.GetNamespace() {
			return nil, dataLossError(fmt.Errorf("activity query payload crosses the requested namespace"))
		}
		if req.Msg.GetWorkflowId() != "" && key.WorkflowID != req.Msg.GetWorkflowId() {
			return nil, dataLossError(fmt.Errorf("activity query payload crosses the requested workflow_id"))
		}
		if req.Msg.GetRunId() != "" && key.RunID != req.Msg.GetRunId() {
			return nil, dataLossError(fmt.Errorf("activity query payload crosses the requested run_id"))
		}
		if req.Msg.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_UNSPECIFIED && record.GetStatus() != req.Msg.GetStatus() {
			return nil, dataLossError(fmt.Errorf("activity query payload does not match the requested status"))
		}
	}
	return connect.NewResponse(response), nil
}

func (handler *QueryHandler) Sweep(ctx context.Context, req *connect.Request[temporalessv1.SweepRequest]) (*connect.Response[temporalessv1.SweepResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	response, err := handler.Query.Sweep(ctx, req.Msg)
	if err != nil {
		return nil, sweepConnectError(err)
	}
	return connect.NewResponse(response), nil
}

func sweepConnectError(err error) error {
	switch {
	case errors.Is(err, storage.ErrInvalidQuery):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, janitor.ErrClaimRunListingUnsupported):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, janitor.ErrRunListingDataLoss), errors.Is(err, storage.ErrCorruptRecord):
		return connect.NewError(connect.CodeDataLoss, err)
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
}

func (handler *QueryHandler) DueTimers(ctx context.Context, req *connect.Request[temporalessv1.RecordQueryServiceDueTimersRequest]) (*connect.Response[temporalessv1.RecordQueryServiceDueTimersResponse], error) {
	if err := validateRPCRequest(req.Msg); err != nil {
		return nil, err
	}
	response, err := handler.Query.DueTimersQuery(ctx, req.Msg)
	if err != nil {
		return nil, queryConnectError(err)
	}
	for _, entry := range response.GetDue() {
		if _, err := validateDueTimerEntry(entry, req.Msg.GetNamespace(), req.Msg.GetNow().AsTime()); err != nil {
			return nil, dataLossError(err)
		}
	}
	return connect.NewResponse(response), nil
}

func queryConnectError(err error) error {
	if errors.Is(err, storage.ErrInvalidQuery) {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	if errors.Is(err, storage.ErrCorruptRecord) {
		return connect.NewError(connect.CodeDataLoss, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

func validateRPCRequest(message proto.Message) error {
	if err := protovalidate.Validate(message); err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	return nil
}

func dataLossError(err error) error {
	return connect.NewError(connect.CodeDataLoss, err)
}

func eventDeliveryConnectError(err error, key storage.EventKey) error {
	if errors.Is(err, storage.ErrCorruptRecord) {
		return dataLossError(err)
	}
	var reason temporalessv1.EventDeliveryFailureReason
	switch {
	case errors.Is(err, storage.ErrEventDeliveryUnsupported):
		reason = temporalessv1.EventDeliveryFailureReason_EVENT_DELIVERY_FAILURE_REASON_UNSUPPORTED
	case errors.Is(err, storage.ErrEventDeliveryConflict):
		reason = temporalessv1.EventDeliveryFailureReason_EVENT_DELIVERY_FAILURE_REASON_CONFLICT
	default:
		return connect.NewError(connect.CodeInternal, err)
	}
	connectErr := connect.NewError(connect.CodeFailedPrecondition, err)
	detail, detailErr := connect.NewErrorDetail(&temporalessv1.EventDeliveryErrorDetail{
		Reason: reason,
		Key:    key.Proto(),
	})
	if detailErr != nil {
		return connect.NewError(connect.CodeInternal, errors.Join(err, detailErr))
	}
	connectErr.AddDetail(detail)
	return connectErr
}

func storeConnectError(err error) error {
	if errors.Is(err, storage.ErrCorruptRecord) {
		return dataLossError(err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
