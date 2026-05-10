package workflow

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

// HandleConnect adapts WrapWorkflow to a ConnectRPC handler signature so a
// service method can be a one-line workflow:
//
//	func (s *pricesService) FetchPrices(
//	    ctx context.Context, req *connect.Request[pricesv1.FetchRequest],
//	) (*connect.Response[pricesv1.FetchResponse], error) {
//	    return workflow.HandleConnect(ctx, req, workflow.WorkflowWrapOptions[*pricesv1.FetchRequest, *pricesv1.FetchResponse]{
//	        Store: s.store,
//	        OptionsFor: func(_ context.Context, r *pricesv1.FetchRequest) (*workflow.Options, error) {
//	            return &workflow.Options{
//	                WorkflowId:  "prices:" + r.GetSymbol(),
//	                RunId:       r.GetRunId(),
//	                CodeVersion: codeVersion(),
//	            }, nil
//	        },
//	        NewResult: func() *pricesv1.FetchResponse { return &pricesv1.FetchResponse{} },
//	        Execute:   fetchPricesBody,
//	    })
//	}
//
// The Execute body uses the standard Go context — call workflow.ExecuteActivity,
// workflow.Sleep, workflow.WaitEvent on it like in any normal workflow body.
// There is no Temporaless-specific handler shape: the framework is a thin
// wrapper around the existing ConnectRPC/gRPC handler model.
//
// Error mapping is applied automatically: framework typed errors are
// translated to *connect.Error via ErrorToConnectCode and wrap the original
// error, so callers can both observe the right gRPC code and recover the
// underlying type via errors.As. Unknown errors pass through unchanged.
func HandleConnect[
	Req any,
	Resp any,
	ReqPtr interface {
		*Req
		proto.Message
	},
	RespPtr interface {
		*Resp
		proto.Message
	},
](
	ctx context.Context,
	req *connect.Request[Req],
	opts WorkflowWrapOptions[ReqPtr, RespPtr],
) (*connect.Response[Resp], error) {
	handler := WrapWorkflow(opts)
	out, err := handler(ctx, ReqPtr(req.Msg))
	if err != nil {
		if code, _, ok := ErrorToConnectCode(err); ok {
			return nil, connect.NewError(code, err)
		}
		return nil, err
	}
	return connect.NewResponse((*Resp)(out)), nil
}

// ErrorToConnectCode maps a workflow error to the appropriate ConnectRPC code.
// Returns the code, a message, and ok=true when the error is one of the
// framework's typed errors; ok=false for unknown errors.
//
// Standard mapping (mirrors `temporaless.workflow_error_to_connect_code` in
// Python and `docs/deployment.md`):
//
//   - *TimerPendingError, *EventPendingError → CodeUnavailable (caller should
//     retry later — workflow stays IN_PROGRESS).
//   - *ClaimBusyError → CodeAlreadyExists (another worker holds the claim).
//   - ErrWorkflowConflict, ErrActivityConflict, ErrTimerConflict → CodeFailedPrecondition.
//   - *ActivityError → CodeInternal with the original code preserved.
//
// HandleConnect already applies this mapping internally; call this directly
// only when you're not using HandleConnect (e.g. driving WrapWorkflow yourself
// behind a non-Connect transport) and need to translate the error.
func ErrorToConnectCode(err error) (connect.Code, string, bool) {
	var timerPending *TimerPendingError
	if errors.As(err, &timerPending) {
		return connect.CodeUnavailable, timerPending.Error(), true
	}
	var eventPending *EventPendingError
	if errors.As(err, &eventPending) {
		return connect.CodeUnavailable, eventPending.Error(), true
	}
	var claimBusy *ClaimBusyError
	if errors.As(err, &claimBusy) {
		return connect.CodeAlreadyExists, claimBusy.Error(), true
	}
	if errors.Is(err, ErrWorkflowConflict) ||
		errors.Is(err, ErrActivityConflict) ||
		errors.Is(err, ErrTimerConflict) {
		return connect.CodeFailedPrecondition, err.Error(), true
	}
	var activityErr *ActivityError
	if errors.As(err, &activityErr) {
		return connect.CodeInternal, activityErr.Error(), true
	}
	return 0, "", false
}
