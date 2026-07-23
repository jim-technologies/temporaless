package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrEventDeliveryUnsupported = errors.New("atomic event delivery is unsupported")
	ErrEventDeliveryConflict    = errors.New("event already contains a different payload")
)

type EventDeliveryConflictError struct {
	Key EventKey
}

func (err *EventDeliveryConflictError) Error() string {
	return fmt.Sprintf(
		"event %q/%q/%q already contains a different payload",
		err.Key.WorkflowID,
		err.Key.RunID,
		err.Key.EventID,
	)
}

func (err *EventDeliveryConflictError) Unwrap() error {
	return ErrEventDeliveryConflict
}

// SendEvent is the application-facing create-once event helper. It rejects
// stores that cannot atomically establish the first payload. PutEvent remains
// the low-level replace primitive for storage services and migration tools.
func SendEvent(ctx context.Context, store EventDeliveryStore, key EventKey, payload proto.Message) error {
	_, err := DeliverEvent(ctx, store, key, payload)
	return err
}

// DeliverEvent atomically establishes the first payload for key. Identical
// duplicates are idempotent and retain the original received_at timestamp.
// A backend without native create-if-absent support returns
// ErrEventDeliveryUnsupported rather than using check-then-write.
func DeliverEvent(
	ctx context.Context,
	store EventDeliveryStore,
	key EventKey,
	payload proto.Message,
) (EventDeliveryDisposition, error) {
	if store == nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			fmt.Errorf("event delivery store is required")
	}
	if err := key.Validate(); err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	if isNilEventPayload(payload) {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			fmt.Errorf("event payload is required")
	}
	capability, err := store.EventDeliveryCapability(ctx)
	if err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	switch capability {
	case temporalessv1.EventDeliveryCapability_EVENT_DELIVERY_CAPABILITY_UNSPECIFIED,
		NoAtomicEventDelivery:
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			ErrEventDeliveryUnsupported
	case CreateOnlyEventDelivery:
	default:
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			fmt.Errorf("event delivery store returned invalid capability %s", capability)
	}
	packed := &anypb.Any{}
	if err := anypb.MarshalFrom(
		packed,
		payload,
		proto.MarshalOptions{Deterministic: true},
	); err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	record := &temporalessv1.EventRecord{
		SchemaVersion: EventRecordSchemaVersion,
		Key:           key.Proto(),
		Payload:       packed,
		ReceivedAt:    timestamppb.New(time.Now().UTC()),
	}
	if err := ValidateEventDeliveryRecord(record, key); err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	disposition, err := store.DeliverEvent(ctx, record)
	if err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	switch disposition {
	case temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED,
		temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_IDEMPOTENT:
		return disposition, nil
	default:
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED,
			fmt.Errorf("event delivery store returned invalid disposition %s", disposition)
	}
}

func isNilEventPayload(payload proto.Message) bool {
	if payload == nil {
		return true
	}
	value := reflect.ValueOf(payload)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// SameEventPayload compares the immutable identity and byte-identical Any
// payload of two deliveries. DeliverEvent canonicalizes payload bytes before
// calling the store. received_at is deliberately excluded: the first
// timestamp remains authoritative for an idempotent duplicate.
func SameEventPayload(left, right *temporalessv1.EventRecord) bool {
	if left == nil || right == nil {
		return false
	}
	return proto.Equal(left.GetKey(), right.GetKey()) &&
		proto.Equal(left.GetPayload(), right.GetPayload())
}
