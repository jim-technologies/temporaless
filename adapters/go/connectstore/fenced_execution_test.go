package connectstore

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

func TestReservedFencingRPCsRefuseOverHTTP(t *testing.T) {
	// A nil embedded store panics on any legacy point operation. Unsupported
	// operations must stop at the transport boundary without touching it.
	handler := NewHandler(struct{ storage.Store }{})
	_, service := temporalessv1connect.NewRecordStoreServiceHandler(handler)
	server := httptest.NewServer(service)
	defer server.Close()
	client := temporalessv1connect.NewRecordStoreServiceClient(server.Client(), server.URL)
	ctx := context.Background()
	response, err := client.GetStoreCapabilities(ctx, connect.NewRequest(&temporalessv1.GetStoreCapabilitiesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if response.Msg.GetFencedExecutionCapability() != temporalessv1.FencedExecutionCapability_FENCED_EXECUTION_CAPABILITY_UNSUPPORTED || response.Msg.GetFencedExecutionStoreIncarnation() != "" {
		t.Fatalf("unexpected advertised fencing: %v", response.Msg)
	}
	tests := []struct {
		name string
		call func() error
	}{
		{"acquire", func() error {
			_, err := client.AcquireExecution(ctx, connect.NewRequest(&temporalessv1.AcquireExecutionRequest{}))
			return err
		}},
		{"renew", func() error {
			_, err := client.RenewExecution(ctx, connect.NewRequest(&temporalessv1.RenewExecutionRequest{}))
			return err
		}},
		{"release", func() error {
			_, err := client.ReleaseExecution(ctx, connect.NewRequest(&temporalessv1.ReleaseExecutionRequest{}))
			return err
		}},
		{"mutations", func() error {
			_, err := client.ApplyExecutionMutations(ctx, connect.NewRequest(&temporalessv1.ApplyExecutionMutationsRequest{}))
			return err
		}},
		{"receipt", func() error {
			_, err := client.GetExecutionOperation(ctx, connect.NewRequest(&temporalessv1.GetExecutionOperationRequest{}))
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); connect.CodeOf(err) != connect.CodeUnimplemented {
				t.Fatalf("error=%v, want UNIMPLEMENTED", err)
			}
		})
	}
}

type fencingCapabilityClient struct {
	temporalessv1connect.RecordStoreServiceClient
	capability temporalessv1.FencedExecutionCapability
}

func (client fencingCapabilityClient) GetStoreCapabilities(context.Context, *connect.Request[temporalessv1.GetStoreCapabilitiesRequest]) (*connect.Response[temporalessv1.GetStoreCapabilitiesResponse], error) {
	return connect.NewResponse(&temporalessv1.GetStoreCapabilitiesResponse{FencedExecutionCapability: client.capability}), nil
}

func TestClientNeverSilentlyEnablesFencing(t *testing.T) {
	tests := []struct {
		name     string
		value    temporalessv1.FencedExecutionCapability
		rejected bool
	}{
		{"old server omitted capability", 0, false},
		{"unsupported", 1, false},
		{"reserved future support", 2, true},
		{"unknown support", 99, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := NewClientStore(fencingCapabilityClient{capability: test.value})
			capability, err := client.FencedExecutionCapability(context.Background())
			if test.rejected {
				if !errors.Is(err, storage.ErrFencedExecutionUnsupported) {
					t.Fatalf("error=%v, want unsupported", err)
				}
			} else if err != nil || capability != temporalessv1.FencedExecutionCapability_FENCED_EXECUTION_CAPABILITY_UNSUPPORTED {
				t.Fatalf("capability=%v error=%v", capability, err)
			}
		})
	}
}
