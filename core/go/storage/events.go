package storage

import (
	"context"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SendEvent packs `payload` as an Any, builds an EventRecord with the current
// time, and writes it via `store.PutEvent`. Use this from external services
// (webhooks, approval handlers, manual operator actions) to deliver a signal
// to a workflow that is waiting via WaitEvent.
func SendEvent(ctx context.Context, store EventStore, key EventKey, payload proto.Message) error {
	if err := key.Validate(); err != nil {
		return err
	}
	packed, err := anypb.New(payload)
	if err != nil {
		return err
	}
	return store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: EventRecordSchemaVersion,
		Key:           key.Proto(),
		Payload:       packed,
		ReceivedAt:    timestamppb.New(time.Now().UTC()),
	})
}
