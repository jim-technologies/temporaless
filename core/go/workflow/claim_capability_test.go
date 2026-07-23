package workflow

import (
	"context"
	"errors"
	"testing"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRunRejectsOptionsUnsupportedByClaimCapability(t *testing.T) {
	tests := []struct {
		name       string
		options    *Options
		capability storage.ClaimCapability
		wantOption string
	}{
		{
			name: "claim owner with no claims",
			options: &Options{
				WorkflowId: "capability-owner", RunId: "run",
				ClaimOwnerId: "worker",
			},
			capability: storage.NoClaims,
			wantOption: "claim_owner_id",
		},
		{
			name: "concurrency with unspecified capability",
			options: &Options{
				WorkflowId: "capability-concurrency", RunId: "run",
				ClaimOwnerId: "worker", ConcurrencyKey: "vendor", ConcurrencyLimit: 1,
			},
			capability: temporalessv1.ClaimCapability_CLAIM_CAPABILITY_UNSPECIFIED,
			wantOption: "concurrency_key",
		},
		{
			name: "claim owner with reserved CAS capability",
			options: &Options{
				WorkflowId: "capability-cas", RunId: "run",
				ClaimOwnerId: "worker",
			},
			capability: storage.CASClaims,
			wantOption: "claim_owner_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claims := &capabilityOverrideClaimStore{
				ClaimStore: newTestClaimStore(t),
				capability: test.capability,
			}
			bodyCalled := false
			_, err := Run(
				ctx, store, test.options, claims,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					bodyCalled = true
					return wrapperspb.String("unexpected"), nil
				},
			)
			if !errors.Is(err, ErrClaimCapabilityUnsupported) {
				t.Fatalf("error = %v, want ErrClaimCapabilityUnsupported", err)
			}
			var capabilityErr *ClaimCapabilityError
			if !errors.As(err, &capabilityErr) {
				t.Fatalf("error type = %T, want *ClaimCapabilityError", err)
			}
			if capabilityErr.Option != test.wantOption || capabilityErr.Capability != test.capability {
				t.Fatalf("capability error = %#v", capabilityErr)
			}
			if bodyCalled {
				t.Fatal("workflow body ran without supported claim coordination")
			}
			if _, found, getErr := store.GetWorkflow(ctx, storage.NewWorkflowKey(
				test.options.GetWorkflowId(), test.options.GetRunId(),
			)); getErr != nil {
				t.Fatal(getErr)
			} else if found {
				t.Fatal("unsupported claim option wrote a workflow record")
			}
		})
	}
}

func TestRunTerminalReplayDoesNotRequireClaimCapability(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	base := &Options{WorkflowId: "capability-replay", RunId: "run"}
	first, err := Run(
		ctx, store, base, nil,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("stored"), nil
		},
	)
	if err != nil || first.GetValue() != "stored" {
		t.Fatalf("first run = %v, %v", first, err)
	}

	replayOptions := protoCloneWorkflowOptions(base)
	replayOptions.ClaimOwnerId = "worker"
	capabilityErr := errors.New("capability endpoint unavailable")
	claims := &capabilityOverrideClaimStore{
		ClaimStore:    newTestClaimStore(t),
		capabilityErr: capabilityErr,
	}
	replayed, err := Run(
		ctx, store, replayOptions, claims,
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			t.Fatal("terminal replay executed workflow body")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.GetValue() != "stored" {
		t.Fatalf("replay value = %q, want stored", replayed.GetValue())
	}
}

type capabilityOverrideClaimStore struct {
	storage.ClaimStore
	capability    storage.ClaimCapability
	capabilityErr error
}

func (store *capabilityOverrideClaimStore) ClaimCapability(
	context.Context,
) (storage.ClaimCapability, error) {
	return store.capability, store.capabilityErr
}

func protoCloneWorkflowOptions(options *Options) *Options {
	return &Options{
		WorkflowId:       options.GetWorkflowId(),
		RunId:            options.GetRunId(),
		ClaimOwnerId:     options.GetClaimOwnerId(),
		ConcurrencyKey:   options.GetConcurrencyKey(),
		ConcurrencyLimit: options.GetConcurrencyLimit(),
	}
}
