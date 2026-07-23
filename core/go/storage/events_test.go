package storage_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// atomicEventStore is a process-local test double for a backend that exposes a
// real create-if-absent primitive. Production Go OpenDAL deliberately does not
// claim this capability; production callers use an atomic service adapter.
type atomicEventStore struct {
	*storage.OpenDALStore
	mu sync.Mutex
}

type invalidCapabilityEventStore struct {
	*atomicEventStore
}

type invalidDispositionEventStore struct {
	*atomicEventStore
}

func (store *invalidDispositionEventStore) DeliverEvent(
	context.Context,
	*temporalessv1.EventRecord,
) (storage.EventDeliveryDisposition, error) {
	return storage.EventDeliveryDisposition(99), nil
}

func (store *invalidCapabilityEventStore) EventDeliveryCapability(
	context.Context,
) (storage.EventDeliveryCapability, error) {
	return storage.EventDeliveryCapability(99), nil
}

func (store *atomicEventStore) EventDeliveryCapability(
	context.Context,
) (storage.EventDeliveryCapability, error) {
	return storage.CreateOnlyEventDelivery, nil
}

func (store *atomicEventStore) DeliverEvent(
	ctx context.Context,
	record *temporalessv1.EventRecord,
) (storage.EventDeliveryDisposition, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	key := storage.EventKeyFromProto(record.GetKey())
	existing, found, err := store.GetEvent(ctx, key)
	if err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	if found {
		if err := storage.ValidateEventDeliveryRecord(existing, key); err != nil {
			return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
		}
		if storage.SameEventPayload(existing, record) {
			return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_IDEMPOTENT, nil
		}
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			&storage.EventDeliveryConflictError{Key: key}
	}
	if err := store.PutEvent(ctx, proto.Clone(record).(*temporalessv1.EventRecord)); err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED, nil
}

func TestOpenDALEventDeliveryFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := newStore(t)

	capability, err := store.EventDeliveryCapability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capability != storage.NoAtomicEventDelivery {
		t.Fatalf("capability = %s, want %s", capability, storage.NoAtomicEventDelivery)
	}
	err = storage.SendEvent(
		ctx,
		store,
		storage.NewEventKey("workflow", "run", "approval"),
		wrapperspb.String("approved"),
	)
	if !errors.Is(err, storage.ErrEventDeliveryUnsupported) {
		t.Fatalf("SendEvent error = %v, want ErrEventDeliveryUnsupported", err)
	}
}

func TestDeliverEventRejectsTypedNilPayloadBeforeStoreCall(t *testing.T) {
	ctx := context.Background()
	store := &atomicEventStore{OpenDALStore: newStore(t)}
	var payload *wrapperspb.StringValue

	_, err := storage.DeliverEvent(
		ctx,
		store,
		storage.NewEventKey("workflow", "run", "approval"),
		payload,
	)
	if err == nil || err.Error() != "event payload is required" {
		t.Fatalf("error=%v, want event payload required", err)
	}
	events, listErr := store.ListEvents(
		ctx,
		storage.NewWorkflowKey("workflow", "run"),
	)
	if listErr != nil {
		t.Fatal(listErr)
	}
	if len(events) != 0 {
		t.Fatalf("typed-nil delivery wrote %d events", len(events))
	}
}

func TestDeliverEventRejectsUnknownLocalCapability(t *testing.T) {
	store := &invalidCapabilityEventStore{
		atomicEventStore: &atomicEventStore{OpenDALStore: newStore(t)},
	}
	_, err := storage.DeliverEvent(
		context.Background(),
		store,
		storage.NewEventKey("workflow", "run", "approval"),
		wrapperspb.String("approved"),
	)
	if err == nil || errors.Is(err, storage.ErrEventDeliveryUnsupported) {
		t.Fatalf("error=%v, want invalid capability error", err)
	}
}

func TestDeliverEventRejectsUnknownLocalDisposition(t *testing.T) {
	store := &invalidDispositionEventStore{
		atomicEventStore: &atomicEventStore{OpenDALStore: newStore(t)},
	}
	_, err := storage.DeliverEvent(
		context.Background(),
		store,
		storage.NewEventKey("workflow", "run", "approval"),
		wrapperspb.String("approved"),
	)
	if err == nil || errors.Is(err, storage.ErrEventDeliveryConflict) {
		t.Fatalf("error=%v, want invalid disposition error", err)
	}
}

func TestDeliverEventCreatedIdempotentAndConflict(t *testing.T) {
	ctx := context.Background()
	store := &atomicEventStore{OpenDALStore: newStore(t)}
	key := storage.NewEventKey("workflow", "run", "approval")

	firstPayload, err := structpb.NewStruct(map[string]any{
		"symbol": "AAPL",
		"prices": map[string]any{"open": 200.0, "close": 204.0},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := storage.DeliverEvent(ctx, store, key, firstPayload)
	if err != nil || first != temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED {
		t.Fatalf("first disposition = %s, err=%v", first, err)
	}
	storedFirst, found, err := store.GetEvent(ctx, key)
	if err != nil || !found {
		t.Fatalf("first read found=%v err=%v", found, err)
	}

	// Construct the same map-bearing payload independently. Deterministic Any
	// packing makes its bytes canonical, so this retry is idempotent.
	retryPayload, err := structpb.NewStruct(map[string]any{
		"prices": map[string]any{"close": 204.0, "open": 200.0},
		"symbol": "AAPL",
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := storage.DeliverEvent(ctx, store, key, retryPayload)
	if err != nil ||
		retry != temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_IDEMPOTENT {
		t.Fatalf("retry disposition = %s, err=%v", retry, err)
	}
	storedRetry, found, err := store.GetEvent(ctx, key)
	if err != nil || !found {
		t.Fatalf("retry read found=%v err=%v", found, err)
	}
	if !proto.Equal(storedFirst.GetReceivedAt(), storedRetry.GetReceivedAt()) {
		t.Fatal("idempotent retry replaced the original received_at")
	}

	_, err = storage.DeliverEvent(ctx, store, key, wrapperspb.String("different"))
	if !errors.Is(err, storage.ErrEventDeliveryConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrEventDeliveryConflict", err)
	}
	var conflict *storage.EventDeliveryConflictError
	if !errors.As(err, &conflict) || conflict.Key != key {
		t.Fatalf("conflict detail = %#v, want key %#v", conflict, key)
	}
}

func TestDeliverEventConcurrentConflictHasSingleWinner(t *testing.T) {
	ctx := context.Background()
	store := &atomicEventStore{OpenDALStore: newStore(t)}
	key := storage.NewEventKey("workflow", "run", "approval")
	start := make(chan struct{})
	results := make(chan error, 2)

	for _, value := range []string{"approved", "rejected"} {
		go func(value string) {
			<-start
			_, err := storage.DeliverEvent(ctx, store, key, wrapperspb.String(value))
			results <- err
		}(value)
	}
	close(start)

	created := 0
	conflicts := 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			created++
		case errors.Is(err, storage.ErrEventDeliveryConflict):
			conflicts++
		default:
			t.Fatalf("unexpected delivery error: %v", err)
		}
	}
	if created != 1 || conflicts != 1 {
		t.Fatalf("created=%d conflicts=%d, want 1/1", created, conflicts)
	}
}
