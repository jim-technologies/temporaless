package storage

import (
	"context"
	"errors"
	"fmt"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
)

// ErrFencedExecutionUnsupported refuses requested fencing without falling back
// to an unfenced write. The contract foundation has no qualified implementation.
var ErrFencedExecutionUnsupported = errors.New("fenced execution is not implemented")

// FencedExecutionStore is the reserved atomic run-ownership boundary. A future
// implementation must enforce the complete protobuf contract, including refusal
// of tokenless mutations and fenced timer repair/retention. No bundled store
// implements it; adding a conditional claim beside mutable records is insufficient.
type FencedExecutionStore interface {
	GetStoreCapabilities(context.Context, *temporalessv1.GetStoreCapabilitiesRequest) (*temporalessv1.GetStoreCapabilitiesResponse, error)
	AcquireExecution(context.Context, *temporalessv1.AcquireExecutionRequest) (*temporalessv1.AcquireExecutionResponse, error)
	RenewExecution(context.Context, *temporalessv1.RenewExecutionRequest) (*temporalessv1.RenewExecutionResponse, error)
	ReleaseExecution(context.Context, *temporalessv1.ReleaseExecutionRequest) (*temporalessv1.ReleaseExecutionResponse, error)
	ApplyExecutionMutations(context.Context, *temporalessv1.ApplyExecutionMutationsRequest) (*temporalessv1.ApplyExecutionMutationsResponse, error)
	GetExecutionOperation(context.Context, *temporalessv1.GetExecutionOperationRequest) (*temporalessv1.GetExecutionOperationResponse, error)
}

// CurrentFencedExecutionCapability interprets old responses conservatively and
// rejects both unknown and reserved future support until this runtime qualifies.
func CurrentFencedExecutionCapability(value temporalessv1.FencedExecutionCapability) (temporalessv1.FencedExecutionCapability, error) {
	switch value {
	case temporalessv1.FencedExecutionCapability_FENCED_EXECUTION_CAPABILITY_UNSPECIFIED,
		temporalessv1.FencedExecutionCapability_FENCED_EXECUTION_CAPABILITY_UNSUPPORTED:
		return temporalessv1.FencedExecutionCapability_FENCED_EXECUTION_CAPABILITY_UNSUPPORTED, nil
	default:
		return 0, fmt.Errorf("%w: capability %s", ErrFencedExecutionUnsupported, value)
	}
}
