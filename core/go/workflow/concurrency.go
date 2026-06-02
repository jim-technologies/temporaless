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
// Maps to gRPC code RESOURCE_EXHAUSTED via ErrorToConnectCode.
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

// concurrencyOwnerID returns the stable owner identity for the workflow
// holding the slot. Using "workflow_id:run_id" lets a crashed invocation's
// next invocation re-acquire its previously-held slot.
func concurrencyOwnerID(workflowID, runID string) string {
	return workflowID + ":" + runID
}

// acquireConcurrencySlot tries slots 0..limit-1 in order; returns the slot_id
// of the acquired slot or empty string when all slots are taken by other
// owners. A slot held by the same owner is treated as re-acquired (crash
// recovery) — this prevents one workflow from consuming multiple slots across
// crash boundaries.
//
// Lease duration is the safety valve: if the worker dies mid-execution, the
// slot is held until the lease expires. The slot is normally released
// explicitly via releaseConcurrencySlot before the lease matters.
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
		// Slot is taken — see whether it's our own stale claim from a prior
		// invocation that crashed before releasing.
		existing, found, err := claimStore.GetClaim(ctx, slotKey)
		if err != nil {
			return "", err
		}
		if found && existing.GetOwnerId() == ownerID {
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
