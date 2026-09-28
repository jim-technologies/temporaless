package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRunRefusesFencingBeforeStoreOrBody(t *testing.T) {
	tests := []struct {
		name       string
		claimOwner string
	}{
		{name: "unclaimed"},
		{name: "legacy claims", claimOwner: "worker"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Every store method would panic if called through this nil interface.
			store := struct{ storage.Store }{}
			_, err := Run(context.Background(), store, &Options{
				WorkflowId: "workflow", RunId: "run", ClaimOwnerId: test.claimOwner,
				FencedExecution: &temporalessv1.FencedExecutionOptions{
					OwnerId: "worker", AcquisitionId: "invocation", LeaseDuration: durationpb.New(30 * time.Second),
				},
			}, nil, wrapperspb.String("request"), func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					t.Fatal("unsupported fencing entered workflow body")
					return nil, nil
				})
			if !errors.Is(err, storage.ErrFencedExecutionUnsupported) {
				t.Fatalf("error=%v, want unsupported", err)
			}
		})
	}
}
