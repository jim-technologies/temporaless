package connectstore

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

type Handler struct {
	Store      storage.Store
	ClaimStore storage.ClaimStore
}

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

func NewHTTPHandler(store storage.Store, opts ...connect.HandlerOption) (string, http.Handler) {
	return temporalessv1connect.NewRecordStoreServiceHandler(NewHandler(store), opts...)
}

func NewHTTPHandlerWithClaims(store storage.Store, claimStore storage.ClaimStore, opts ...connect.HandlerOption) (string, http.Handler) {
	return temporalessv1connect.NewRecordStoreServiceHandler(NewHandlerWithClaims(store, claimStore), opts...)
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

func (handler *Handler) Sweep(ctx context.Context, req *connect.Request[temporalessv1.SweepRequest]) (*connect.Response[temporalessv1.SweepResponse], error) {
	deleted, err := handler.Store.Sweep(
		ctx,
		req.Msg.GetNamespace(),
		req.Msg.GetNow().AsTime(),
		req.Msg.GetMaxAge().AsDuration(),
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
