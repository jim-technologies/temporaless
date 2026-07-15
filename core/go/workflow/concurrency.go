package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ConcurrencyWorkflowID is the synthetic workflow_id under which concurrency
// slot claims are stored. Sourced from the proto-declared default on
// `ReservedNames.concurrency_workflow_id` so the SDK and the proto contract
// can't drift.
var ConcurrencyWorkflowID = temporalessv1.Default_ReservedNames_ConcurrencyWorkflowId

// concurrencySlotIDPrefix prefixes each slot's claim_id. Combined with the
// slot index (`slot:0`, `slot:1`, ...) by the runtime. Same single-source-of-
// truth pattern as ConcurrencyWorkflowID.
var concurrencySlotIDPrefix = temporalessv1.Default_ReservedNames_ConcurrencySlotIdPrefix

// ErrConcurrencyBusy is the sentinel for ConcurrencyBusyError-class errors.
var ErrConcurrencyBusy = errors.New("concurrency cap is busy")

// ConcurrencyBusyError is raised when a workflow's `concurrency_key` slot pool
// is full. The workflow body did NOT execute and no IN_PROGRESS record was
// written — callers retry the same workflow.Run when capacity is available.
//
// Transport adapters may map this to their standard capacity-exhausted status.
type ConcurrencyBusyError struct {
	Key   string
	Limit uint32
}

func (err *ConcurrencyBusyError) Error() string {
	return fmt.Sprintf("concurrency cap %q at limit %d", err.Key, err.Limit)
}

func (err *ConcurrencyBusyError) Unwrap() error {
	return ErrConcurrencyBusy
}

// acquireConcurrencySlot tries slots 0..limit-1 in order; returns the slot_id
// of the newly acquired slot or empty string when all slots are occupied.
// Existing slots are never treated as acquired, even when owner_id matches.
//
// The lease timestamp is diagnostic for create-only stores; expiry and owner
// equality do not grant takeover. Normal exits release the slot explicitly;
// a leaked slot requires verified operator cleanup.
func acquireConcurrencySlot(
	ctx context.Context,
	claimStore storage.ClaimStore,
	namespace string,
	concurrencyKey string,
	limit uint32,
	ownerID string,
	codeVersion string,
	leaseDuration time.Duration,
) (string, error) {
	for i := uint32(0); i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		slotID := fmt.Sprintf("%s%d", concurrencySlotIDPrefix, i)
		slotKey := storage.ClaimKey{
			Namespace:  namespace,
			WorkflowID: ConcurrencyWorkflowID,
			RunID:      concurrencyKey,
			ClaimID:    slotID,
		}
		now := time.Now().UTC()
		claim := &temporalessv1.ClaimRecord{
			SchemaVersion:  storage.ClaimRecordSchemaVersion,
			Key:            slotKey.Proto(),
			OwnerId:        ownerID,
			ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
			ResourceId:     concurrencyKey,
			CodeVersion:    codeVersion,
			LeaseExpiresAt: timestamppb.New(now.Add(leaseDuration)),
			CreatedAt:      timestamppb.New(now),
			HeartbeatAt:    timestamppb.New(now),
		}
		created, err := claimStore.TryCreateClaim(ctx, claim)
		if err != nil {
			return "", err
		}
		if created {
			return slotID, nil
		}
	}
	return "", nil
}

// releaseConcurrencySlot deletes the named slot claim. Idempotent: a missing
// claim is not an error. Always called via defer once a slot is acquired, so
// that every exit path (success, failure, pending) releases the slot.
func releaseConcurrencySlot(
	ctx context.Context,
	claimStore storage.ClaimStore,
	namespace string,
	concurrencyKey string,
	slotID string,
) error {
	slotKey := storage.ClaimKey{
		Namespace:  namespace,
		WorkflowID: ConcurrencyWorkflowID,
		RunID:      concurrencyKey,
		ClaimID:    slotID,
	}
	_, err := claimStore.DeleteClaim(ctx, slotKey)
	return err
}
