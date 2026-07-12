package workflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Concurrency keys: pre-emptive cluster-wide cap on in-flight workflow.Run
// invocations sharing the same key. Tests cover the storage-arbitrated
// acquire race, busy-on-full, release-on-completion / failure / pending,
// and create-only stale-slot behavior.

func TestConcurrencyAcquireWhenFree(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	opts := &temporalessv1.WorkflowOptions{
		WorkflowId:       "wf",
		RunId:            "r-1",
		CodeVersion:      "test",
		ClaimOwnerId:     "worker:free",
		ConcurrencyKey:   "vendor:test",
		ConcurrencyLimit: 3,
	}
	result, err := Run(
		ctx, store, opts, claimStore,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String("ok:" + in.GetValue()), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "ok:x" {
		t.Fatalf("result = %q", result.GetValue())
	}

	// Slot must be released on completion — verify the claim is gone.
	slotKey := storage.ClaimKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: ConcurrencyWorkflowID,
		RunID:      "vendor:test",
		ClaimID:    "slot:0",
	}
	_, found, err := claimStore.GetClaim(ctx, slotKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("slot:0 should be released after workflow completion")
	}
}

func TestConcurrencyBusyWhenFull(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	// Pre-fill all slots with other-owner claims (simulating 2 workflows in flight).
	for i := 0; i < 2; i++ {
		slotKey := storage.ClaimKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: ConcurrencyWorkflowID,
			RunID:      "vendor:test",
			ClaimID:    "slot:" + intToASCII(int32(i)),
		}
		now := time.Now().UTC()
		_, err := claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
			SchemaVersion:  storage.ClaimRecordSchemaVersion,
			Key:            slotKey.Proto(),
			OwnerId:        "other-workflow:other-run",
			ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
			ResourceId:     "vendor:test",
			LeaseExpiresAt: timestamppb.New(now.Add(15 * time.Minute)),
			CreatedAt:      timestamppb.New(now),
			HeartbeatAt:    timestamppb.New(now),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	opts := &temporalessv1.WorkflowOptions{
		WorkflowId:       "wf",
		RunId:            "r-1",
		CodeVersion:      "test",
		ClaimOwnerId:     "worker:busy",
		ConcurrencyKey:   "vendor:test",
		ConcurrencyLimit: 2,
	}
	executed := false
	_, err := Run(
		ctx, store, opts, claimStore,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, in *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			executed = true
			return wrapperspb.String("ok"), nil
		},
	)
	if err == nil {
		t.Fatal("expected ConcurrencyBusyError")
	}
	var busy *ConcurrencyBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("err type = %T, want *ConcurrencyBusyError", err)
	}
	if executed {
		t.Fatal("body must not execute when concurrency-busy")
	}

	// IN_PROGRESS record must not exist — busy is a no-side-effect condition.
	wfKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "wf",
		RunID:      "r-1",
	}
	_, found, err := store.GetWorkflow(ctx, wfKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("no IN_PROGRESS record should be written when busy")
	}
}

func TestConcurrencyReleasedOnFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	opts := &temporalessv1.WorkflowOptions{
		WorkflowId:       "wf",
		RunId:            "r-failed",
		CodeVersion:      "test",
		ClaimOwnerId:     "worker:failure",
		ConcurrencyKey:   "vendor:test",
		ConcurrencyLimit: 2,
	}
	_, err := Run(
		ctx, store, opts, claimStore,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, errors.New("body failed")
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}

	// Slot must be released even on failure.
	slotKey := storage.ClaimKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: ConcurrencyWorkflowID,
		RunID:      "vendor:test",
		ClaimID:    "slot:0",
	}
	_, found, err := claimStore.GetClaim(ctx, slotKey)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("slot should be released after workflow failure")
	}
}

func TestConcurrencyMultipleWorkflowsObeyLimit(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	// 5 workflows, limit=2. Block 2 in the body via a channel; the other 3
	// should observe ConcurrencyBusyError.
	const limit = 2
	const total = 5
	gate := make(chan struct{}) // closed to release blocked bodies

	var (
		inflight    atomic.Int64
		maxInflight atomic.Int64
		busy        atomic.Int64
		succeeded   atomic.Int64
		wg          sync.WaitGroup
	)
	wg.Add(total)
	for i := 0; i < total; i++ {
		i := i
		go func() {
			defer wg.Done()
			opts := &temporalessv1.WorkflowOptions{
				WorkflowId:       "wf",
				RunId:            "r-" + intToASCII(int32(i)),
				CodeVersion:      "test",
				ClaimOwnerId:     "worker:" + intToASCII(int32(i)),
				ConcurrencyKey:   "vendor:test",
				ConcurrencyLimit: limit,
			}
			_, err := Run(
				ctx, store, opts, claimStore,
				wrapperspb.String("x"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					n := inflight.Add(1)
					for {
						current := maxInflight.Load()
						if n <= current || maxInflight.CompareAndSwap(current, n) {
							break
						}
					}
					<-gate
					inflight.Add(-1)
					return wrapperspb.String("ok"), nil
				},
			)
			if err != nil {
				var b *ConcurrencyBusyError
				if errors.As(err, &b) {
					busy.Add(1)
					return
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
			succeeded.Add(1)
		}()
	}

	// Wait briefly so the racers get their acquire results, then release the gate.
	time.Sleep(100 * time.Millisecond)
	close(gate)
	wg.Wait()

	if got := maxInflight.Load(); got > limit {
		t.Errorf("max in-flight = %d, want <= %d", got, limit)
	}
	if succeeded.Load()+busy.Load() != total {
		t.Errorf("succeeded+busy = %d+%d, want %d", succeeded.Load(), busy.Load(), total)
	}
	if succeeded.Load() == 0 {
		t.Error("expected at least one workflow to succeed")
	}
}

func TestConcurrencyStaleSameOwnerSlotIsNotReacquired(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	// Simulate a prior crashed invocation. Matching owner text is not fencing:
	// treating it as re-acquired would also let two live duplicates share and
	// prematurely release this slot.
	ownerID := "worker:same"
	slotKey := storage.ClaimKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: ConcurrencyWorkflowID,
		RunID:      "vendor:test",
		ClaimID:    "slot:0",
	}
	now := time.Now().UTC()
	_, err := claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion:  storage.ClaimRecordSchemaVersion,
		Key:            slotKey.Proto(),
		OwnerId:        ownerID,
		ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_CONCURRENCY_KEY,
		LeaseExpiresAt: timestamppb.New(now.Add(15 * time.Minute)),
		CreatedAt:      timestamppb.New(now),
		HeartbeatAt:    timestamppb.New(now),
	})
	if err != nil {
		t.Fatal(err)
	}

	opts := &temporalessv1.WorkflowOptions{
		WorkflowId:       "wf",
		RunId:            "r-1",
		CodeVersion:      "test",
		ClaimOwnerId:     ownerID,
		ConcurrencyKey:   "vendor:test",
		ConcurrencyLimit: 1,
	}
	executed := false
	_, err = Run(
		ctx, store, opts, claimStore,
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			executed = true
			return wrapperspb.String("ok"), nil
		},
	)
	var busy *ConcurrencyBusyError
	if !errors.As(err, &busy) {
		t.Fatalf("error = %T (%v), want *ConcurrencyBusyError", err, err)
	}
	if executed {
		t.Fatal("workflow body executed through a stale same-owner slot")
	}
	_, found, err := claimStore.GetClaim(ctx, slotKey)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("stale create-only slot was deleted or treated as acquired")
	}
}

func TestConcurrencyRequiresClaimStore(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	opts := &temporalessv1.WorkflowOptions{
		WorkflowId:       "wf",
		RunId:            "r",
		CodeVersion:      "test",
		ClaimOwnerId:     "worker:no-store",
		ConcurrencyKey:   "vendor:test",
		ConcurrencyLimit: 1,
	}
	_, err := Run(
		ctx, store, opts, nil, // <- no claim store
		wrapperspb.String("x"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			t.Fatal("body must not execute")
			return nil, nil
		},
	)
	if err == nil {
		t.Fatal("expected error")
	}
	// Should specifically mention claim store / concurrency.
}

func TestConcurrencyValidationPaired(t *testing.T) {
	// concurrency_key without concurrency_limit (or vice versa) must be
	// rejected by protovalidate's paired CEL constraint.
	tests := []struct {
		name  string
		key   string
		lim   uint32
		owner string
	}{
		{"key without limit", "vendor:x", 0, "worker:validation"},
		{"limit without key", "", 5, "worker:validation"},
		{"key without caller owner", "vendor:x", 1, ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claimStore := newTestClaimStore(t)
			opts := &temporalessv1.WorkflowOptions{
				WorkflowId:       "wf",
				RunId:            "r",
				CodeVersion:      "test",
				ClaimOwnerId:     tc.owner,
				ConcurrencyKey:   tc.key,
				ConcurrencyLimit: tc.lim,
			}
			_, err := Run(
				ctx, store, opts, claimStore,
				wrapperspb.String("x"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("ok"), nil
				},
			)
			if err == nil {
				t.Fatal("expected validation error from paired CEL")
			}
		})
	}
}
