package workflow

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestWrapWorkflowPassesSingleFlightClaimStore(t *testing.T) {
	store := newTestStore(t)
	claims := newTestClaimStore(t)
	executions := 0
	handler := WrapWorkflow(WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
		Store:      store,
		ClaimStore: claims,
		Options: &Options{
			WorkflowId:   "wrapped:claims",
			RunId:        "run:1",
			CodeVersion:  "v1",
			ClaimOwnerId: "worker",
		},
		NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		Execute: func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("wrapped:" + request.GetValue()), nil
		},
	})

	for index := 0; index < 2; index++ {
		result, err := handler(context.Background(), wrapperspb.String("AAPL"))
		if err != nil {
			t.Fatal(err)
		}
		if got := result.GetValue(); got != "wrapped:AAPL" {
			t.Fatalf("result = %q, want wrapped:AAPL", got)
		}
	}
	if executions != 1 {
		t.Fatalf("executions = %d, want 1", executions)
	}
}
