// Package connectworkflow adapts Temporaless workflow execution to ConnectRPC
// handler signatures.
package connectworkflow

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/proto"
)

// Handle adapts workflow.WrapWorkflow to a ConnectRPC handler signature so a
// service method can be a one-line workflow:
//
//	func (s *pricesService) FetchPrices(
//	    ctx context.Context, req *connect.Request[pricesv1.FetchRequest],
//	) (*connect.Response[pricesv1.FetchResponse], error) {
//	    return connectworkflow.Handle(ctx, req, workflow.WorkflowWrapOptions[*pricesv1.FetchRequest, *pricesv1.FetchResponse]{
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
// The Execute body uses the standard Go context. Call workflow.ExecuteActivity,
// workflow.Sleep, and workflow.WaitEvent on it like any other workflow body.
// There is no Temporaless-specific handler shape: this adapter is a thin
// wrapper around the existing ConnectRPC/gRPC handler model.
//
// Framework typed errors are translated to *connect.Error via ErrorToCode and
// wrap the original error, so callers can both observe the right gRPC code and
// recover the underlying type via errors.As. Unknown errors pass through
// unchanged.
func Handle[
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
	opts workflow.WorkflowWrapOptions[ReqPtr, RespPtr],
) (*connect.Response[Resp], error) {
	if req == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("connect workflow request is required"))
	}
	if req.Msg == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("connect workflow request message is required"))
	}
	handler := workflow.WrapWorkflow(opts)
	out, err := handler(ctx, ReqPtr(req.Msg))
	if err != nil {
		if code, _, ok := ErrorToCode(err); ok {
			return nil, connect.NewError(code, err)
		}
		return nil, err
	}
	return connect.NewResponse((*Resp)(out)), nil
}

// ErrorToCode maps a workflow error to the appropriate ConnectRPC code.
// It returns the code, a message, and true when err is one of the framework's
// typed errors; unknown errors return false.
//
// Standard mapping (mirrors temporaless_connectworkflow.error_to_connect_code
// in Python and docs/deployment.md):
//
//   - *workflow.TimerPendingError, *workflow.EventPendingError,
//     *workflow.WorkflowDependencyPendingError, and
//     *workflow.WorkflowInfrastructureError map to CodeUnavailable.
//   - *workflow.ClaimBusyError maps to CodeAlreadyExists.
//   - *workflow.ConcurrencyBusyError maps to CodeResourceExhausted.
//   - workflow.ErrClaimRelease maps to CodeInternal.
//   - *workflow.ClaimCapabilityError and record conflicts map to
//     CodeFailedPrecondition.
//   - *workflow.ActivityError and *workflow.WorkflowDependencyFailedError map
//     to CodeInternal.
//
// Handle applies this mapping internally. Call ErrorToCode directly when
// driving workflow.WrapWorkflow yourself at a ConnectRPC boundary.
func ErrorToCode(err error) (connect.Code, string, bool) {
	if errors.Is(err, workflow.ErrClaimRelease) {
		return connect.CodeInternal, err.Error(), true
	}
	// An activity is an application boundary. Its stored failure may wrap a
	// framework-shaped cause returned by user code; the outer ActivityError must
	// remain authoritative instead of being remapped as a continuation signal.
	var activityErr *workflow.ActivityError
	if errors.As(err, &activityErr) {
		return connect.CodeInternal, activityErr.Error(), true
	}
	if errors.Is(err, workflow.ErrWorkflowConflict) ||
		errors.Is(err, workflow.ErrActivityConflict) ||
		errors.Is(err, workflow.ErrTimerConflict) {
		return connect.CodeFailedPrecondition, err.Error(), true
	}
	var timerPending *workflow.TimerPendingError
	if errors.As(err, &timerPending) {
		return connect.CodeUnavailable, timerPending.Error(), true
	}
	var eventPending *workflow.EventPendingError
	if errors.As(err, &eventPending) {
		return connect.CodeUnavailable, eventPending.Error(), true
	}
	var depPending *workflow.WorkflowDependencyPendingError
	if errors.As(err, &depPending) {
		return connect.CodeUnavailable, depPending.Error(), true
	}
	var infrastructureErr *workflow.WorkflowInfrastructureError
	if errors.As(err, &infrastructureErr) {
		return connect.CodeUnavailable, infrastructureErr.Error(), true
	}
	var claimBusy *workflow.ClaimBusyError
	if errors.As(err, &claimBusy) {
		return connect.CodeAlreadyExists, claimBusy.Error(), true
	}
	var concurrencyBusy *workflow.ConcurrencyBusyError
	if errors.As(err, &concurrencyBusy) {
		return connect.CodeResourceExhausted, concurrencyBusy.Error(), true
	}
	var capabilityErr *workflow.ClaimCapabilityError
	if errors.As(err, &capabilityErr) {
		return connect.CodeFailedPrecondition, capabilityErr.Error(), true
	}
	var depFailed *workflow.WorkflowDependencyFailedError
	if errors.As(err, &depFailed) {
		return connect.CodeInternal, depFailed.Error(), true
	}
	return 0, "", false
}
