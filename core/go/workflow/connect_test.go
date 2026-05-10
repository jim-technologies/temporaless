package workflow_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// pricesService is a fake ConnectRPC service. The "trigger point" of the
// framework: the user writes a normal connect handler, the framework wraps
// each call as a workflow with replay semantics. There is no
// Temporaless-specific handler shape.
type pricesService struct {
	store storage.Store
	calls int
}

func (s *pricesService) FetchPrices(
	ctx context.Context,
	req *connect.Request[wrapperspb.StringValue],
) (*connect.Response[wrapperspb.StringValue], error) {
	return workflow.HandleConnect(
		ctx,
		req,
		workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Store: s.store,
			OptionsFor: func(_ context.Context, r *wrapperspb.StringValue) (*workflow.Options, error) {
				return &workflow.Options{
					WorkflowId:  "prices:" + r.GetValue(),
					RunId:       "2026-05-04",
					CodeVersion: "v1",
				}, nil
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			Execute: func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return workflow.ExecuteActivity(
					ctx,
					&workflow.ActivityOptions{ActivityId: "vendor:" + request.GetValue()},
					request,
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(_ context.Context, req *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						s.calls++
						return wrapperspb.String("vendor:" + req.GetValue()), nil
					},
				)
			},
		},
	)
}

func TestHandleConnect_WrapsConnectMethodAsWorkflow(t *testing.T) {
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	service := &pricesService{store: store}
	ctx := context.Background()

	// First call drives the workflow body and the activity.
	resp, err := service.FetchPrices(ctx, connect.NewRequest(wrapperspb.String("AAPL")))
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got := resp.Msg.GetValue(); got != "vendor:AAPL" {
		t.Fatalf("first call value = %q, want %q", got, "vendor:AAPL")
	}
	if service.calls != 1 {
		t.Fatalf("vendor calls = %d, want 1", service.calls)
	}

	// Second call replays from storage. Same response, no vendor call.
	resp, err = service.FetchPrices(ctx, connect.NewRequest(wrapperspb.String("AAPL")))
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got := resp.Msg.GetValue(); got != "vendor:AAPL" {
		t.Fatalf("replay value = %q, want %q", got, "vendor:AAPL")
	}
	if service.calls != 1 {
		t.Fatalf("vendor calls after replay = %d, want still 1", service.calls)
	}
}

func TestErrorToConnectCodeMapsEachErrorType(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode connect.Code
	}{
		{"timer pending", &workflow.TimerPendingError{TimerID: "t1", WakeAt: time.Now()}, connect.CodeUnavailable},
		{"event pending", &workflow.EventPendingError{EventID: "e1"}, connect.CodeUnavailable},
		{"workflow dep pending", &workflow.WorkflowDependencyPendingError{WorkflowID: "upstream", RunID: "2026-05-04"}, connect.CodeUnavailable},
		{"claim busy", &workflow.ClaimBusyError{ClaimID: "activity:fetch"}, connect.CodeAlreadyExists},
		{"workflow conflict", workflow.ErrWorkflowConflict, connect.CodeFailedPrecondition},
		{"activity conflict", workflow.ErrActivityConflict, connect.CodeFailedPrecondition},
		{"timer conflict", workflow.ErrTimerConflict, connect.CodeFailedPrecondition},
		{"activity error", workflow.NewActivityError("rate_limited", "vendor 429", nil), connect.CodeInternal},
		{"workflow dep failed", &workflow.WorkflowDependencyFailedError{WorkflowID: "upstream", RunID: "2026-05-04", Status: 3}, connect.CodeInternal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, msg, ok := workflow.ErrorToConnectCode(test.err)
			if !ok {
				t.Fatalf("expected mapping for %T", test.err)
			}
			if code != test.wantCode {
				t.Fatalf("code = %v, want %v", code, test.wantCode)
			}
			if msg == "" {
				t.Fatal("expected non-empty message")
			}
		})
	}

	// Unknown error returns ok=false so caller decides.
	if _, _, ok := workflow.ErrorToConnectCode(errors.New("foo")); ok {
		t.Fatal("expected ok=false for unknown error")
	}
}

// pendingService models a workflow body that always raises TimerPendingError —
// the durable-sleep "come back later" signal. HandleConnect must translate
// this to *connect.Error{CodeUnavailable} so Connect clients with standard
// retry policy back off and re-call. The original typed error is preserved
// via wrapping so errors.As still works.
type pendingService struct {
	store storage.Store
}

func (s *pendingService) FetchPrices(
	ctx context.Context,
	req *connect.Request[wrapperspb.StringValue],
) (*connect.Response[wrapperspb.StringValue], error) {
	return workflow.HandleConnect(
		ctx, req,
		workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Store: s.store,
			OptionsFor: func(_ context.Context, r *wrapperspb.StringValue) (*workflow.Options, error) {
				return &workflow.Options{
					WorkflowId:  "prices:" + r.GetValue(),
					RunId:       "2026-05-04",
					CodeVersion: "v1",
				}, nil
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			Execute: func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				// Sleep 1h every call — first invocation always returns pending.
				if err := workflow.Sleep(ctx, "wait", time.Hour); err != nil {
					return nil, err
				}
				return wrapperspb.String("never"), nil
			},
		},
	)
}

func TestHandleConnect_AutoMapsTimerPendingToUnavailable(t *testing.T) {
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	service := &pendingService{store: store}
	_, err = service.FetchPrices(context.Background(), connect.NewRequest(wrapperspb.String("AAPL")))
	if err == nil {
		t.Fatal("expected pending error, got nil")
	}

	// Auto-mapped to *connect.Error with CodeUnavailable.
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		t.Fatalf("expected *connect.Error, got %T (%v)", err, err)
	}
	if connectErr.Code() != connect.CodeUnavailable {
		t.Fatalf("code = %v, want CodeUnavailable", connectErr.Code())
	}

	// Original typed error is recoverable via errors.As — wrapping preserves it.
	var pending *workflow.TimerPendingError
	if !errors.As(err, &pending) {
		t.Fatalf("expected to recover *TimerPendingError via errors.As, got %T", err)
	}
	if pending.TimerID != "wait" {
		t.Fatalf("recovered TimerID = %q, want %q", pending.TimerID, "wait")
	}
}

func TestHandleConnect_PassesThroughUnknownErrors(t *testing.T) {
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	store := storage.NewOpenDALStore(operator)

	customErr := errors.New("custom unknown error")
	_, err = workflow.HandleConnect(
		context.Background(),
		connect.NewRequest(wrapperspb.String("AAPL")),
		workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Store: store,
			Options: &workflow.Options{
				WorkflowId: "x", RunId: "y", CodeVersion: "v1",
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			Execute: func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return nil, customErr
			},
		},
	)
	if !errors.Is(err, customErr) {
		t.Fatalf("expected unknown error to pass through unchanged, got %v", err)
	}
	// And it should NOT have been wrapped as *connect.Error.
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		t.Fatalf("unknown error should not be wrapped as *connect.Error, got code %v", connectErr.Code())
	}
}
