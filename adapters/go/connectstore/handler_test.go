package connectstore

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	"github.com/jim-technologies/temporaless/adapters/go/scanquery"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type corruptBackendErrorStore struct {
	storage.Store
}

type atomicEventBackend struct {
	storage.Store
	mu sync.Mutex
}

type unspecifiedEventBackend struct {
	*atomicEventBackend
}

type eventDeliveryErrorClient struct {
	temporalessv1connect.RecordStoreServiceClient
	detail proto.Message
}

func (client *eventDeliveryErrorClient) DeliverEvent(
	context.Context,
	*connect.Request[temporalessv1.DeliverEventRequest],
) (*connect.Response[temporalessv1.DeliverEventResponse], error) {
	connectErr := connect.NewError(
		connect.CodeFailedPrecondition,
		errors.New("adversarial event delivery error"),
	)
	detail, err := connect.NewErrorDetail(client.detail)
	if err != nil {
		return nil, err
	}
	connectErr.AddDetail(detail)
	return nil, connectErr
}

func (store *atomicEventBackend) EventDeliveryCapability(
	context.Context,
) (storage.EventDeliveryCapability, error) {
	return storage.CreateOnlyEventDelivery, nil
}

func (store *unspecifiedEventBackend) EventDeliveryCapability(
	context.Context,
) (storage.EventDeliveryCapability, error) {
	return temporalessv1.EventDeliveryCapability_EVENT_DELIVERY_CAPABILITY_UNSPECIFIED, nil
}

func (store *atomicEventBackend) DeliverEvent(
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
	if err := store.PutEvent(ctx, record); err != nil {
		return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_UNSPECIFIED, err
	}
	return temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED, nil
}

func (store *corruptBackendErrorStore) GetTimer(
	context.Context,
	storage.TimerKey,
) (*temporalessv1.TimerRecord, bool, error) {
	return nil, false, fmt.Errorf("decode timer: %w", storage.ErrCorruptRecord)
}

func TestHandlerMapsBackendCorruptionToDataLoss(t *testing.T) {
	key := storage.NewTimerKey("workflow", "run", "timer")
	handler := NewHandler(&corruptBackendErrorStore{Store: newTestStore(t)})
	_, err := handler.GetTimer(context.Background(), connect.NewRequest(&temporalessv1.GetTimerRequest{
		Key: key.Proto(),
	}))
	if connect.CodeOf(err) != connect.CodeDataLoss {
		t.Fatalf("code=%s err=%v, want DATA_LOSS", connect.CodeOf(err), err)
	}
	_, _, err = NewClientStore(handler).GetTimer(context.Background(), key)
	if !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("client err=%v, want ErrCorruptRecord", err)
	}
}

func TestHandlerRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Handler, *anypb.Any) (bool, string, error)
	}{
		{
			name: "workflow",
			run: func(ctx context.Context, handler *Handler, result *anypb.Any) (bool, string, error) {
				key := storage.NewWorkflowKey("prices:aapl", "2026-05-02")
				_, err := handler.PutWorkflow(ctx, connect.NewRequest(&temporalessv1.PutWorkflowRequest{
					Record: &temporalessv1.WorkflowRecord{
						SchemaVersion: storage.WorkflowRecordSchemaVersion,
						Key:           key.Proto(),
						WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
						Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
						Result:        result,
					},
				}))
				if err != nil {
					return false, "", err
				}
				resp, err := handler.GetWorkflow(ctx, connect.NewRequest(&temporalessv1.GetWorkflowRequest{
					Key: key.Proto(),
				}))
				if err != nil {
					return false, "", err
				}
				return resp.Msg.GetFound(), resp.Msg.GetRecord().GetResult().GetTypeUrl(), nil
			},
		},
		{
			name: "activity",
			run: func(ctx context.Context, handler *Handler, result *anypb.Any) (bool, string, error) {
				key := storage.NewActivityKey("prices:aapl", "2026-05-02", "fetch:price")
				_, err := handler.PutActivity(ctx, connect.NewRequest(&temporalessv1.PutActivityRequest{
					Record: &temporalessv1.ActivityRecord{
						SchemaVersion: storage.ActivityRecordSchemaVersion,
						Key:           key.Proto(),
						ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
						Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
						Result:        result,
					},
				}))
				if err != nil {
					return false, "", err
				}
				resp, err := handler.GetActivity(ctx, connect.NewRequest(&temporalessv1.GetActivityRequest{
					Key: key.Proto(),
				}))
				if err != nil {
					return false, "", err
				}
				return resp.Msg.GetFound(), resp.Msg.GetRecord().GetResult().GetTypeUrl(), nil
			},
		},
		{
			name: "timer",
			run: func(ctx context.Context, handler *Handler, _ *anypb.Any) (bool, string, error) {
				key := storage.NewTimerKey("prices:aapl", "2026-05-02", "wait:vendor-window")
				_, err := handler.PutTimer(ctx, connect.NewRequest(&temporalessv1.PutTimerRequest{
					Record: &temporalessv1.TimerRecord{
						SchemaVersion: storage.TimerRecordSchemaVersion,
						Key:           key.Proto(),
						TimerKind:     storage.SleepTimerKind,
						Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
						FireAt:        timestamppb.Now(),
					},
				}))
				if err != nil {
					return false, "", err
				}
				resp, err := handler.GetTimer(ctx, connect.NewRequest(&temporalessv1.GetTimerRequest{
					Key: key.Proto(),
				}))
				if err != nil {
					return false, "", err
				}
				return resp.Msg.GetFound(), resp.Msg.GetRecord().GetTimerKind().String(), nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := NewHandler(newTestStore(t))
			result, err := anypb.New(wrapperspb.String("100.00"))
			if err != nil {
				t.Fatal(err)
			}

			found, typeURL, err := test.run(context.Background(), handler, result)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("record not found")
			}
			wantType := result.GetTypeUrl()
			if test.name == "timer" {
				wantType = storage.SleepTimerKind.String()
			}
			if typeURL != wantType {
				t.Fatalf("result type = %q, want %q", typeURL, wantType)
			}
		})
	}
}

type latestPointerPointStore struct {
	storage.Store
}

func TestClientStoreLatestWorkflowRunUsesPointRPC(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	key := storage.NewWorkflowKey("prices:aapl", "2026-07-03T09:00:00Z")
	completedAt := timestamppb.New(time.Date(2026, 7, 3, 9, 1, 0, 0, time.UTC))
	if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     completedAt,
		CompletedAt:   completedAt,
	}); err != nil {
		t.Fatal(err)
	}
	pointStore := &latestPointerPointStore{Store: backend}
	clientStore := NewClientStore(NewHandler(pointStore))

	pointer, found, err := clientStore.GetLatestWorkflowRun(ctx, "", key.WorkflowID)
	if err != nil || !found {
		t.Fatalf("GetLatestWorkflowRun: found=%v err=%v", found, err)
	}
	if got := pointer.GetKey().GetRunId(); got != key.RunID {
		t.Fatalf("run_id = %q, want %q", got, key.RunID)
	}
	_, found, err = clientStore.GetLatestWorkflowRun(ctx, "", "prices:missing")
	if err != nil || found {
		t.Fatalf("missing pointer: found=%v err=%v", found, err)
	}
}

func TestClientStoreUsesRecordStoreService(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	_, handler := NewHTTPHandler(backend)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	clientStore := NewHTTPClientStore(server.Client(), server.URL)
	capability, err := clientStore.ClaimCapability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capability != storage.NoClaims {
		t.Fatalf("claim capability = %s, want %s", capability, storage.NoClaims)
	}

	key := storage.NewWorkflowKey("prices:rpc", "2026-05-02")
	err = clientStore.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	})
	if err != nil {
		t.Fatal(err)
	}

	record, found, err := clientStore.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("workflow record not found")
	}
	if record.GetWorkflowType() != "workflow:google.protobuf.StringValue->google.protobuf.StringValue" {
		t.Fatalf("workflow type = %q", record.GetWorkflowType())
	}
}

func TestClientStoreDeliverEventPropagatesCapabilityAndTypedOutcomes(t *testing.T) {
	ctx := context.Background()
	backend := &atomicEventBackend{Store: newTestStore(t)}
	clientStore := NewClientStore(NewHandler(backend))

	capability, err := clientStore.EventDeliveryCapability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capability != storage.CreateOnlyEventDelivery {
		t.Fatalf("capability = %s, want %s", capability, storage.CreateOnlyEventDelivery)
	}
	key := storage.NewEventKey("workflow", "run", "approval")
	first, err := storage.DeliverEvent(ctx, clientStore, key, wrapperspb.String("approved"))
	if err != nil ||
		first != temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_CREATED {
		t.Fatalf("first disposition=%s err=%v", first, err)
	}
	retry, err := storage.DeliverEvent(ctx, clientStore, key, wrapperspb.String("approved"))
	if err != nil ||
		retry != temporalessv1.EventDeliveryDisposition_EVENT_DELIVERY_DISPOSITION_IDEMPOTENT {
		t.Fatalf("retry disposition=%s err=%v", retry, err)
	}
	_, err = storage.DeliverEvent(ctx, clientStore, key, wrapperspb.String("rejected"))
	if !errors.Is(err, storage.ErrEventDeliveryConflict) {
		t.Fatalf("conflict error=%v, want ErrEventDeliveryConflict", err)
	}
	var conflict *storage.EventDeliveryConflictError
	if !errors.As(err, &conflict) || conflict.Key != key {
		t.Fatalf("conflict detail=%#v, want key %#v", conflict, key)
	}
}

func TestClientStoreDeliverEventFailsClosedForGoOpenDAL(t *testing.T) {
	ctx := context.Background()
	clientStore := NewClientStore(NewHandler(newTestStore(t)))
	capability, err := clientStore.EventDeliveryCapability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capability != storage.NoAtomicEventDelivery {
		t.Fatalf("capability = %s, want %s", capability, storage.NoAtomicEventDelivery)
	}
	err = storage.SendEvent(
		ctx,
		clientStore,
		storage.NewEventKey("workflow", "run", "approval"),
		wrapperspb.String("approved"),
	)
	if !errors.Is(err, storage.ErrEventDeliveryUnsupported) {
		t.Fatalf("SendEvent error=%v, want ErrEventDeliveryUnsupported", err)
	}
}

func TestClientStoreTreatsUnspecifiedEventDeliveryCapabilityAsUnsupported(t *testing.T) {
	ctx := context.Background()
	backend := &unspecifiedEventBackend{
		atomicEventBackend: &atomicEventBackend{Store: newTestStore(t)},
	}
	clientStore := NewClientStore(NewHandler(backend))

	capability, err := clientStore.EventDeliveryCapability(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if capability != storage.NoAtomicEventDelivery {
		t.Fatalf("capability = %s, want %s", capability, storage.NoAtomicEventDelivery)
	}
	err = storage.SendEvent(
		ctx,
		clientStore,
		storage.NewEventKey("workflow", "run", "approval"),
		wrapperspb.String("approved"),
	)
	if !errors.Is(err, storage.ErrEventDeliveryUnsupported) {
		t.Fatalf("SendEvent error=%v, want ErrEventDeliveryUnsupported", err)
	}
}

func TestClientStoreRejectsMalformedEventDeliveryErrorDetail(t *testing.T) {
	ctx := context.Background()
	key := storage.NewEventKey("workflow", "run", "approval")
	payload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	record := &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           key.Proto(),
		Payload:       payload,
		ReceivedAt:    timestamppb.Now(),
	}
	tests := []struct {
		name   string
		detail proto.Message
	}{
		{
			name: "invalid key",
			detail: &temporalessv1.EventDeliveryErrorDetail{
				Reason: temporalessv1.EventDeliveryFailureReason_EVENT_DELIVERY_FAILURE_REASON_CONFLICT,
				Key: &temporalessv1.EventKey{
					Namespace:  storage.DefaultNamespace,
					WorkflowId: ".",
					RunId:      "run",
					EventId:    "approval",
				},
			},
		},
		{
			name: "mismatched key",
			detail: &temporalessv1.EventDeliveryErrorDetail{
				Reason: temporalessv1.EventDeliveryFailureReason_EVENT_DELIVERY_FAILURE_REASON_CONFLICT,
				Key:    storage.NewEventKey("workflow", "run", "other").Proto(),
			},
		},
		{
			name: "unspecified reason",
			detail: &temporalessv1.EventDeliveryErrorDetail{
				Reason: temporalessv1.EventDeliveryFailureReason_EVENT_DELIVERY_FAILURE_REASON_UNSPECIFIED,
				Key:    key.Proto(),
			},
		},
		{
			name: "undecodable detail",
			detail: &anypb.Any{
				TypeUrl: "type.googleapis.com/temporaless.v1.EventDeliveryErrorDetail",
				Value:   []byte{0xff},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewClientStore(&eventDeliveryErrorClient{detail: test.detail})
			_, err := store.DeliverEvent(ctx, record)
			if !errors.Is(err, storage.ErrCorruptRecord) {
				t.Fatalf("error=%v, want ErrCorruptRecord", err)
			}
		})
	}
}

func TestHandlerDeliverEventRequiresPayloadAndReceivedAt(t *testing.T) {
	handler := NewHandler(&atomicEventBackend{Store: newTestStore(t)})
	key := storage.NewEventKey("workflow", "run", "approval")
	payload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		record *temporalessv1.EventRecord
	}{
		{
			name: "missing payload",
			record: &temporalessv1.EventRecord{
				SchemaVersion: storage.EventRecordSchemaVersion,
				Key:           key.Proto(),
				ReceivedAt:    timestamppb.Now(),
			},
		},
		{
			name: "missing received_at",
			record: &temporalessv1.EventRecord{
				SchemaVersion: storage.EventRecordSchemaVersion,
				Key:           key.Proto(),
				Payload:       payload,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := handler.DeliverEvent(
				context.Background(),
				connect.NewRequest(&temporalessv1.DeliverEventRequest{
					Record: test.record,
				}),
			)
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code=%s err=%v, want INVALID_ARGUMENT", connect.CodeOf(err), err)
			}
		})
	}
}

func TestClientStoreDeliverEventMapsCorruptExistingRecord(t *testing.T) {
	ctx := context.Background()
	base := newTestStore(t)
	backend := &atomicEventBackend{Store: base}
	key := storage.NewEventKey("workflow", "run", "approval")
	payload, err := anypb.New(wrapperspb.String("approved"))
	if err != nil {
		t.Fatal(err)
	}
	if err := base.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           key.Proto(),
		Payload:       payload,
		// Missing received_at is accepted only by low-level PutEvent.
	}); err != nil {
		t.Fatal(err)
	}

	client := NewClientStore(NewHandler(backend))
	_, err = storage.DeliverEvent(ctx, client, key, wrapperspb.String("approved"))
	if !errors.Is(err, storage.ErrCorruptRecord) {
		t.Fatalf("error=%v, want ErrCorruptRecord", err)
	}
}

func TestHTTPHandlerDoesNotMountLocalQueryFallbackByDefault(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	_, handler := NewHTTPHandler(backend)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	clientStore := NewHTTPClientStore(server.Client(), server.URL)
	if _, err := clientStore.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{}); err == nil {
		t.Fatal("expected default HTTP handler to omit RecordQueryService")
	}
}

func TestLocalClientStoreUsesPointServiceWithoutHTTP(t *testing.T) {
	ctx := context.Background()
	clientStore := NewLocalClientStore(newTestStore(t))

	key := storage.NewWorkflowKey("prices:local", "2026-05-02")
	if err := clientStore.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	}); err != nil {
		t.Fatal(err)
	}

	record, found, err := clientStore.GetWorkflow(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("workflow record not found")
	}
	if record.GetWorkflowType() != "workflow:google.protobuf.StringValue->google.protobuf.StringValue" {
		t.Fatalf("workflow type = %q", record.GetWorkflowType())
	}

	if _, err := clientStore.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{}); err == nil {
		t.Fatal("point-only local client unexpectedly exposed RecordQueryService")
	}
}

func TestQueryHandlerRejectsUnsupportedOrderingAndPagination(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *QueryHandler) error
	}{
		{
			name: "workflow order_by",
			run: func(ctx context.Context, handler *QueryHandler) error {
				_, err := handler.ListWorkflows(ctx, connect.NewRequest(&temporalessv1.ListWorkflowsRequest{
					OrderBy: "created_at desc",
				}))
				return err
			},
		},
		{
			name: "activity page_size",
			run: func(ctx context.Context, handler *QueryHandler) error {
				_, err := handler.ListActivities(ctx, connect.NewRequest(&temporalessv1.RecordQueryServiceListActivitiesRequest{
					PageSize: 10,
				}))
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, query := newTestStoreAndQuery(t, nil)
			err := test.run(context.Background(), NewQueryHandler(query))
			if connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeInvalidArgument, err)
			}
		})
	}
}

func TestHandlersRejectMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "point key",
			run: func() error {
				_, err := NewHandler(newTestStore(t)).GetWorkflow(
					context.Background(),
					connect.NewRequest(&temporalessv1.GetWorkflowRequest{}),
				)
				return err
			},
		},
		{
			name: "point due time",
			run: func() error {
				_, err := NewHandler(newTestStore(t)).DueTimers(
					context.Background(),
					connect.NewRequest(&temporalessv1.DueTimersRequest{}),
				)
				return err
			},
		},
		{
			name: "query due time",
			run: func() error {
				_, err := NewQueryHandler(nil).DueTimers(
					context.Background(),
					connect.NewRequest(&temporalessv1.RecordQueryServiceDueTimersRequest{}),
				)
				return err
			},
		},
		{
			name: "sweep time and age",
			run: func() error {
				_, err := NewQueryHandler(nil).Sweep(
					context.Background(),
					connect.NewRequest(&temporalessv1.SweepRequest{}),
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); connect.CodeOf(err) != connect.CodeInvalidArgument {
				t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeInvalidArgument, err)
			}
		})
	}
}

func TestClientStoreListAndDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend, query := newTestStoreAndQuery(t, nil)
	_, handler := NewHTTPHandlerWithLocalQuery(backend, query)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	clientStore := NewHTTPClientStore(server.Client(), server.URL)

	keepKey := storage.NewWorkflowKey("prices:keep", "2026-05-02")
	dropKey := storage.NewWorkflowKey("prices:drop", "2026-05-02")
	for _, key := range []storage.WorkflowKey{keepKey, dropKey} {
		if err := clientStore.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
			SchemaVersion: storage.WorkflowRecordSchemaVersion,
			Key:           key.Proto(),
			WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
			Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		}); err != nil {
			t.Fatal(err)
		}
	}

	response, err := clientStore.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{Status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED})
	if err != nil {
		t.Fatal(err)
	}
	records := response.GetRecords()
	if got := len(records); got != 2 {
		t.Fatalf("list count = %d, want 2", got)
	}

	deleted, err := clientStore.DeleteWorkflow(ctx, dropKey)
	if err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("expected delete to report true")
	}

	response, err = clientStore.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{Status: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED})
	if err != nil {
		t.Fatal(err)
	}
	records = response.GetRecords()
	if got := len(records); got != 1 {
		t.Fatalf("list count after delete = %d, want 1", got)
	}
	if records[0].GetKey().GetWorkflowId() != "prices:keep" {
		t.Fatalf("remaining workflow = %q", records[0].GetKey().GetWorkflowId())
	}

	// Idempotency: deleting the already-gone record returns false, no error.
	deleted, err = clientStore.DeleteWorkflow(ctx, dropKey)
	if err != nil {
		t.Fatal(err)
	}
	if deleted {
		t.Fatal("expected delete on missing record to report false")
	}
}

func TestClientStoreRoundTripsAllRecordTypes(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	claimStore := newClaimsBackend(t)
	_, handler := NewHTTPHandlerWithClaims(backend, claimStore)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	clientStore := NewHTTPClientStore(server.Client(), server.URL)
	wfKey := storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "rpc:roundtrip",
		RunID:      "2026-05-04",
	}

	// Timer round-trip
	timerKey := storage.TimerKey{
		Namespace:  wfKey.Namespace,
		WorkflowID: wfKey.WorkflowID,
		RunID:      wfKey.RunID,
		TimerID:    "wait",
	}
	if err := clientStore.PutTimer(ctx, &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           timerKey.Proto(),
		TimerKind:     storage.SleepTimerKind,
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:        timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	timer, found, err := clientStore.GetTimer(ctx, timerKey)
	if err != nil || !found || timer.GetTimerKind() != storage.SleepTimerKind {
		t.Fatalf("GetTimer: found=%v err=%v", found, err)
	}
	timers, err := clientStore.ListTimers(ctx, wfKey, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil || len(timers) != 1 {
		t.Fatalf("ListTimers: count=%d err=%v", len(timers), err)
	}
	deleted, err := clientStore.DeleteTimer(ctx, timerKey)
	if err != nil || !deleted {
		t.Fatalf("DeleteTimer: deleted=%v err=%v", deleted, err)
	}

	// Low-level event point-record round-trip. Atomic external delivery has
	// separate capability/DeliverEvent coverage below.
	eventKey := storage.EventKey{
		Namespace:  wfKey.Namespace,
		WorkflowID: wfKey.WorkflowID,
		RunID:      wfKey.RunID,
		EventID:    "approval",
	}
	payload, err := anypb.New(wrapperspb.String("manager"))
	if err != nil {
		t.Fatal(err)
	}
	if err := clientStore.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           eventKey.Proto(),
		Payload:       payload,
		ReceivedAt:    timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	events, err := clientStore.ListEvents(ctx, wfKey)
	if err != nil || len(events) != 1 {
		t.Fatalf("ListEvents: count=%d err=%v", len(events), err)
	}
	got := &wrapperspb.StringValue{}
	if err := events[0].GetPayload().UnmarshalTo(got); err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "manager" {
		t.Fatalf("event payload = %q", got.GetValue())
	}
	deleted, err = clientStore.DeleteEvent(ctx, eventKey)
	if err != nil || !deleted {
		t.Fatalf("DeleteEvent: deleted=%v err=%v", deleted, err)
	}

	// Claim round-trip via TryCreateClaim
	claimKey := storage.ClaimKey{
		Namespace:  wfKey.Namespace,
		WorkflowID: wfKey.WorkflowID,
		RunID:      wfKey.RunID,
		ClaimID:    "claim-1",
	}
	created, err := clientStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner-1",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
		ResourceId:    "claim-1",
	})
	if err != nil || !created {
		t.Fatalf("TryCreateClaim first: created=%v err=%v", created, err)
	}
	created, err = clientStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner-2",
	})
	if err != nil || created {
		t.Fatalf("TryCreateClaim second: created=%v err=%v (expected created=false)", created, err)
	}
	claim, found, err := clientStore.GetClaim(ctx, claimKey)
	if err != nil || !found || claim.GetOwnerId() != "owner-1" {
		t.Fatalf("GetClaim: owner=%q found=%v err=%v", claim.GetOwnerId(), found, err)
	}
}

func TestClientStoreDeleteRunDeletesClaimsFromExplicitStore(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	claimStore := newClaimsBackend(t)
	_, handler := NewHTTPHandlerWithClaims(backend, claimStore)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	clientStore := NewHTTPClientStore(server.Client(), server.URL)

	key := storage.NewWorkflowKey("prices:delete-run", "run:one")
	activityKey := storage.NewActivityKey(key.WorkflowID, key.RunID, "fetch")
	if err := clientStore.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	}); err != nil {
		t.Fatal(err)
	}
	if err := clientStore.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           activityKey.Proto(),
		ActivityType:  "activity:test",
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
	}); err != nil {
		t.Fatal(err)
	}
	for _, claimID := range []string{"arbitrary:one", "arbitrary:two"} {
		claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, claimID)
		created, err := clientStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
			SchemaVersion: storage.ClaimRecordSchemaVersion,
			Key:           claimKey.Proto(),
			OwnerId:       "owner",
			ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
			ResourceId:    claimID,
		})
		if err != nil || !created {
			t.Fatalf("create claim %q: created=%v err=%v", claimID, created, err)
		}
	}

	claims, err := clientStore.ListClaims(ctx, key)
	if err != nil || len(claims) != 2 {
		t.Fatalf("ListClaims before delete: count=%d err=%v", len(claims), err)
	}
	deleted, err := clientStore.DeleteRun(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 4 {
		t.Fatalf("deleted = %d, want 4", deleted)
	}
	if _, found, err := clientStore.GetWorkflow(ctx, key); err != nil || found {
		t.Fatalf("GetWorkflow after delete: found=%v err=%v", found, err)
	}
	if _, found, err := clientStore.GetActivity(ctx, activityKey); err != nil || found {
		t.Fatalf("GetActivity after delete: found=%v err=%v", found, err)
	}
	claims, err = clientStore.ListClaims(ctx, key)
	if err != nil || len(claims) != 0 {
		t.Fatalf("ListClaims after delete: count=%d err=%v", len(claims), err)
	}
}

func TestClientStoreSweepDeletesClaimsFromExplicitStore(t *testing.T) {
	ctx := context.Background()
	claimStore := newClaimsBackend(t)
	backend, query := newTestStoreAndQuery(t, claimStore)
	_, handler := NewHTTPHandlerWithClaimsAndLocalQuery(backend, claimStore, query)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	clientStore := NewHTTPClientStore(server.Client(), server.URL)
	key := storage.NewWorkflowKey("prices:sweep", "run:old")
	old := timestamppb.New(time.Now().Add(-48 * time.Hour))
	if err := clientStore.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     old,
		CompletedAt:   old,
	}); err != nil {
		t.Fatal(err)
	}
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "stale")
	created, err := clientStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	})
	if err != nil || !created {
		t.Fatalf("create claim: created=%v err=%v", created, err)
	}

	response, err := clientStore.Sweep(ctx, &temporalessv1.SweepRequest{
		Now:    timestamppb.Now(),
		MaxAge: durationpb.New(24 * time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	deleted := response.GetDeleted()
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if _, found, err := clientStore.GetWorkflow(ctx, key); err != nil || found {
		t.Fatalf("workflow after sweep: found=%v err=%v", found, err)
	}
	if _, found, err := clientStore.GetClaim(ctx, claimKey); err != nil || found {
		t.Fatalf("claim after sweep: found=%v err=%v", found, err)
	}
}

type pointOnlyClaimStore struct {
	storage.ClaimStore
}

type noClaimsPointStore struct {
	storage.ClaimStore
}

func (store *noClaimsPointStore) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return storage.NoClaims, nil
}

type capabilityOverrideClaimStore struct {
	storage.ClaimStore
	capability storage.ClaimCapability
}

func (store *capabilityOverrideClaimStore) ClaimCapability(context.Context) (storage.ClaimCapability, error) {
	return store.capability, nil
}

type corruptListingClaimStore struct {
	storage.ClaimRunStore
	records     []*temporalessv1.ClaimRecord
	deleteCalls int
}

func (store *corruptListingClaimStore) ListClaims(context.Context, storage.WorkflowKey) ([]*temporalessv1.ClaimRecord, error) {
	return store.records, nil
}

func (store *corruptListingClaimStore) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	store.deleteCalls++
	return store.ClaimRunStore.DeleteClaim(ctx, key)
}

type countingClaimRunStore struct {
	storage.ClaimRunStore
	deleteCalls int
}

func (store *countingClaimRunStore) DeleteClaim(ctx context.Context, key storage.ClaimKey) (bool, error) {
	store.deleteCalls++
	return store.ClaimRunStore.DeleteClaim(ctx, key)
}

type corruptRunListingStore struct {
	storage.Store
	activities  []*temporalessv1.ActivityRecord
	timers      []*temporalessv1.TimerRecord
	events      []*temporalessv1.EventRecord
	deleteCalls int
}

func (store *corruptRunListingStore) ListActivities(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.ActivityRecord, error) {
	if store.activities != nil {
		return store.activities, nil
	}
	return store.Store.ListActivities(ctx, key)
}

func (store *corruptRunListingStore) ListTimers(ctx context.Context, key storage.WorkflowKey, status temporalessv1.TimerStatus) ([]*temporalessv1.TimerRecord, error) {
	if store.timers != nil {
		return store.timers, nil
	}
	return store.Store.ListTimers(ctx, key, status)
}

func (store *corruptRunListingStore) ListEvents(ctx context.Context, key storage.WorkflowKey) ([]*temporalessv1.EventRecord, error) {
	if store.events != nil {
		return store.events, nil
	}
	return store.Store.ListEvents(ctx, key)
}

func (store *corruptRunListingStore) DeleteActivity(ctx context.Context, key storage.ActivityKey) (bool, error) {
	store.deleteCalls++
	return store.Store.DeleteActivity(ctx, key)
}

func (store *corruptRunListingStore) DeleteTimer(ctx context.Context, key storage.TimerKey) (bool, error) {
	store.deleteCalls++
	return store.Store.DeleteTimer(ctx, key)
}

func (store *corruptRunListingStore) DeleteEvent(ctx context.Context, key storage.EventKey) (bool, error) {
	store.deleteCalls++
	return store.Store.DeleteEvent(ctx, key)
}

func (store *corruptRunListingStore) DeleteWorkflow(ctx context.Context, key storage.WorkflowKey) (bool, error) {
	store.deleteCalls++
	return store.Store.DeleteWorkflow(ctx, key)
}

func TestHandlerDeleteRunRejectsPointOnlyClaimStoreBeforeMutation(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	claimStore := newClaimsBackend(t)
	pointOnly := &pointOnlyClaimStore{ClaimStore: claimStore}
	handler := NewHandlerWithClaims(backend, pointOnly)
	key := storage.NewWorkflowKey("prices:delete-run", "run:point-only")
	if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	}); err != nil {
		t.Fatal(err)
	}
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "arbitrary")
	created, err := pointOnly.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	})
	if err != nil || !created {
		t.Fatalf("create claim: created=%v err=%v", created, err)
	}

	_, err = handler.DeleteRun(ctx, connect.NewRequest(&temporalessv1.DeleteRunRequest{
		Key: key.Proto(),
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeFailedPrecondition, err)
	}
	if _, found, getErr := backend.GetWorkflow(ctx, key); getErr != nil || !found {
		t.Fatalf("workflow mutated before rejection: found=%v err=%v", found, getErr)
	}
	if _, found, getErr := claimStore.GetClaim(ctx, claimKey); getErr != nil || !found {
		t.Fatalf("claim mutated before rejection: found=%v err=%v", found, getErr)
	}
}

func TestClientStoreSweepRejectsPointOnlyClaimStoreBeforeMutation(t *testing.T) {
	ctx := context.Background()
	claimStore := newClaimsBackend(t)
	pointOnly := &pointOnlyClaimStore{ClaimStore: claimStore}
	backend, query := newTestStoreAndQuery(t, pointOnly)
	_, handler := NewHTTPHandlerWithClaimsAndLocalQuery(backend, pointOnly, query)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	clientStore := NewHTTPClientStore(server.Client(), server.URL)
	key := storage.NewWorkflowKey("prices:sweep", "run:point-only")
	old := timestamppb.New(time.Now().Add(-48 * time.Hour))
	if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     old,
		CompletedAt:   old,
	}); err != nil {
		t.Fatal(err)
	}
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain")
	created, err := claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	})
	if err != nil || !created {
		t.Fatalf("create claim: created=%v err=%v", created, err)
	}

	_, err = clientStore.Sweep(ctx, &temporalessv1.SweepRequest{
		Now:    timestamppb.Now(),
		MaxAge: durationpb.New(24 * time.Hour),
	})
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeFailedPrecondition, err)
	}
	if _, found, getErr := backend.GetWorkflow(ctx, key); getErr != nil || !found {
		t.Fatalf("workflow mutated before rejection: found=%v err=%v", found, getErr)
	}
	if _, found, getErr := claimStore.GetClaim(ctx, claimKey); getErr != nil || !found {
		t.Fatalf("claim mutated before rejection: found=%v err=%v", found, getErr)
	}
}

func TestHandlerDeleteRunTreatsNoClaimsCapabilityAsRecordOnly(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	claimStore := &noClaimsPointStore{ClaimStore: newClaimsBackend(t)}
	handler := NewHandlerWithClaims(backend, claimStore)
	key := storage.NewWorkflowKey("prices:delete-run", "run:no-claims")
	if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
	}); err != nil {
		t.Fatal(err)
	}

	listed, err := handler.ListClaims(ctx, connect.NewRequest(&temporalessv1.ListClaimsRequest{
		Key: key.Proto(),
	}))
	if err != nil || len(listed.Msg.GetRecords()) != 0 {
		t.Fatalf("ListClaims: count=%d err=%v", len(listed.Msg.GetRecords()), err)
	}
	deleted, err := handler.DeleteRun(ctx, connect.NewRequest(&temporalessv1.DeleteRunRequest{
		Key: key.Proto(),
	}))
	if err != nil {
		t.Fatal(err)
	}
	if deleted.Msg.GetDeleted() != 1 {
		t.Fatalf("deleted = %d, want 1", deleted.Msg.GetDeleted())
	}
}

func TestHandlerDeleteRunValidatesEntireClaimListingBeforeDelete(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	claims := newClaimsBackend(t)
	key := storage.NewWorkflowKey("prices:delete-run", "run:corrupt")
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "target")
	target := &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	}
	created, err := claims.TryCreateClaim(ctx, target)
	if err != nil || !created {
		t.Fatalf("create target claim: created=%v err=%v", created, err)
	}
	corrupt := &corruptListingClaimStore{
		ClaimRunStore: claims,
		records: []*temporalessv1.ClaimRecord{
			target,
			{
				SchemaVersion: storage.ClaimRecordSchemaVersion,
				Key:           storage.NewClaimKey(key.WorkflowID, "run:other", "misplaced").Proto(),
				OwnerId:       "owner",
				ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
				ResourceId:    key.WorkflowID,
			},
		},
	}
	handler := NewHandlerWithClaims(backend, corrupt)

	_, err = handler.DeleteRun(ctx, connect.NewRequest(&temporalessv1.DeleteRunRequest{
		Key: key.Proto(),
	}))
	if connect.CodeOf(err) != connect.CodeDataLoss {
		t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeDataLoss, err)
	}
	if corrupt.deleteCalls != 0 {
		t.Fatalf("DeleteClaim calls = %d, want 0", corrupt.deleteCalls)
	}
	if _, found, getErr := claims.GetClaim(ctx, claimKey); getErr != nil || !found {
		t.Fatalf("target claim mutated before full validation: found=%v err=%v", found, getErr)
	}
}

func TestQueryHandlerSweepMapsCorruptClaimListingToDataLoss(t *testing.T) {
	ctx := context.Background()
	claims := newClaimsBackend(t)
	key := storage.NewWorkflowKey("prices:sweep", "run:corrupt")
	old := timestamppb.New(time.Now().Add(-48 * time.Hour))
	claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "target")
	target := &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           claimKey.Proto(),
		OwnerId:       "owner",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	}
	created, err := claims.TryCreateClaim(ctx, target)
	if err != nil || !created {
		t.Fatalf("create claim: created=%v err=%v", created, err)
	}
	corrupt := &corruptListingClaimStore{
		ClaimRunStore: claims,
		records: []*temporalessv1.ClaimRecord{
			target,
			{Key: storage.NewClaimKey(key.WorkflowID, "run:other", "misplaced").Proto()},
		},
	}
	backend, query := newTestStoreAndQuery(t, corrupt)
	if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     old,
		CompletedAt:   old,
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewQueryHandler(query)

	_, err = handler.Sweep(ctx, connect.NewRequest(&temporalessv1.SweepRequest{
		Now:    timestamppb.Now(),
		MaxAge: durationpb.New(24 * time.Hour),
	}))
	if connect.CodeOf(err) != connect.CodeDataLoss {
		t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeDataLoss, err)
	}
	if corrupt.deleteCalls != 0 {
		t.Fatalf("DeleteClaim calls = %d, want 0", corrupt.deleteCalls)
	}
	if _, found, getErr := backend.GetWorkflow(ctx, key); getErr != nil || !found {
		t.Fatalf("workflow mutated before rejection: found=%v err=%v", found, getErr)
	}
	if _, found, getErr := claims.GetClaim(ctx, claimKey); getErr != nil || !found {
		t.Fatalf("claim mutated before rejection: found=%v err=%v", found, getErr)
	}
}

func TestHandlerDeleteRunValidatesEveryRecordSnapshotBeforeDelete(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*corruptRunListingStore, storage.WorkflowKey)
	}{
		{
			name: "activity mismatched run",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.activities = []*temporalessv1.ActivityRecord{
					{
						Key: &temporalessv1.ActivityKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							ActivityId: "valid",
						},
					},
					{Key: storage.NewActivityKey(key.WorkflowID, "run:other", "misplaced").Proto()},
				}
			},
		},
		{
			name: "activity invalid key",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.activities = []*temporalessv1.ActivityRecord{
					{
						Key: &temporalessv1.ActivityKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							ActivityId: "valid",
						},
					},
					{
						Key: &temporalessv1.ActivityKey{
							Namespace:  storage.DefaultNamespace,
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							ActivityId: "invalid/id",
						},
					},
				}
			},
		},
		{
			name: "timer mismatched run",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.timers = []*temporalessv1.TimerRecord{
					{
						Key: &temporalessv1.TimerKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							TimerId:    "valid",
						},
					},
					{Key: storage.NewTimerKey(key.WorkflowID, "run:other", "misplaced").Proto()},
				}
			},
		},
		{
			name: "timer invalid key",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.timers = []*temporalessv1.TimerRecord{
					{
						Key: &temporalessv1.TimerKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							TimerId:    "valid",
						},
					},
					{
						Key: &temporalessv1.TimerKey{
							Namespace:  storage.DefaultNamespace,
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							TimerId:    "invalid/id",
						},
					},
				}
			},
		},
		{
			name: "event mismatched run",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.events = []*temporalessv1.EventRecord{
					{
						Key: &temporalessv1.EventKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							EventId:    "valid",
						},
					},
					{Key: storage.NewEventKey(key.WorkflowID, "run:other", "misplaced").Proto()},
				}
			},
		},
		{
			name: "event invalid key",
			configure: func(store *corruptRunListingStore, key storage.WorkflowKey) {
				store.events = []*temporalessv1.EventRecord{
					{
						Key: &temporalessv1.EventKey{
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							EventId:    "valid",
						},
					},
					{
						Key: &temporalessv1.EventKey{
							Namespace:  storage.DefaultNamespace,
							WorkflowId: key.WorkflowID,
							RunId:      key.RunID,
							EventId:    "invalid/id",
						},
					},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			backend := newTestStore(t)
			key := storage.NewWorkflowKey("prices:delete-run", "run:corrupt-record")
			if err := backend.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
				SchemaVersion: storage.WorkflowRecordSchemaVersion,
				Key:           key.Proto(),
				WorkflowType:  "workflow:test",
				Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			}); err != nil {
				t.Fatal(err)
			}

			baseClaims := newClaimsBackend(t)
			claims := &countingClaimRunStore{ClaimRunStore: baseClaims}
			claimKey := storage.NewClaimKey(key.WorkflowID, key.RunID, "must-remain")
			created, err := claims.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
				SchemaVersion: storage.ClaimRecordSchemaVersion,
				Key:           claimKey.Proto(),
				OwnerId:       "owner",
				ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
				ResourceId:    key.WorkflowID,
			})
			if err != nil || !created {
				t.Fatalf("create claim: created=%v err=%v", created, err)
			}

			records := &corruptRunListingStore{Store: backend}
			test.configure(records, key)
			handler := NewHandlerWithClaims(records, claims)
			_, err = handler.DeleteRun(ctx, connect.NewRequest(&temporalessv1.DeleteRunRequest{
				Key: key.Proto(),
			}))
			if connect.CodeOf(err) != connect.CodeDataLoss {
				t.Fatalf("code = %s, want %s (err=%v)", connect.CodeOf(err), connect.CodeDataLoss, err)
			}
			if claims.deleteCalls != 0 {
				t.Fatalf("DeleteClaim calls = %d, want 0", claims.deleteCalls)
			}
			if records.deleteCalls != 0 {
				t.Fatalf("record delete calls = %d, want 0", records.deleteCalls)
			}
			if _, found, getErr := backend.GetWorkflow(ctx, key); getErr != nil || !found {
				t.Fatalf("workflow mutated before full validation: found=%v err=%v", found, getErr)
			}
			if _, found, getErr := baseClaims.GetClaim(ctx, claimKey); getErr != nil || !found {
				t.Fatalf("claim mutated before full validation: found=%v err=%v", found, getErr)
			}
		})
	}
}

func newClaimsBackend(t *testing.T) storage.ClaimRunStore {
	t.Helper()
	// MetadataDontWrite — see comment in gocdkclaims/store_test.go for why
	// the sidecar would otherwise cause io.EOF on racing reads.
	bucket, err := fileblob.OpenBucket(t.TempDir(), &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	return gocdkclaims.NewStore(bucket)
}

func TestHandlerReportsNoClaimCapabilityWithoutClaimStore(t *testing.T) {
	handler := NewHandler(newTestStore(t))

	resp, err := handler.GetStoreCapabilities(context.Background(), connect.NewRequest(&temporalessv1.GetStoreCapabilitiesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if resp.Msg.GetClaimCapability() != storage.NoClaims {
		t.Fatalf("claim capability = %s, want %s", resp.Msg.GetClaimCapability(), storage.NoClaims)
	}
}

func TestHandlerAndClientExposeOnlyCurrentClaimCapabilities(t *testing.T) {
	tests := []struct {
		name          string
		capability    storage.ClaimCapability
		want          storage.ClaimCapability
		wantErrorCode connect.Code
	}{
		{
			name:       "no claims remains no claims",
			capability: storage.NoClaims,
			want:       storage.NoClaims,
		},
		{
			name:       "create only remains create only",
			capability: storage.CreateOnlyClaims,
			want:       storage.CreateOnlyClaims,
		},
		{
			name:          "reserved CAS is rejected",
			capability:    storage.CASClaims,
			want:          storage.NoClaims,
			wantErrorCode: connect.CodeFailedPrecondition,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claims := &capabilityOverrideClaimStore{
				ClaimStore: newClaimsBackend(t),
				capability: test.capability,
			}
			handler := NewHandlerWithClaims(newTestStore(t), claims)

			response, err := handler.GetStoreCapabilities(
				context.Background(),
				connect.NewRequest(&temporalessv1.GetStoreCapabilitiesRequest{}),
			)
			if test.wantErrorCode == 0 && err != nil {
				t.Fatalf("handler returned unexpected error: %v", err)
			}
			if test.wantErrorCode != 0 && connect.CodeOf(err) != test.wantErrorCode {
				t.Fatalf(
					"handler code = %s, want %s (err=%v)",
					connect.CodeOf(err),
					test.wantErrorCode,
					err,
				)
			}
			if err == nil && response.Msg.GetClaimCapability() != test.want {
				t.Fatalf(
					"handler capability = %s, want %s",
					response.Msg.GetClaimCapability(),
					test.want,
				)
			}

			got, clientErr := NewClientStore(handler).ClaimCapability(context.Background())
			if test.wantErrorCode == 0 && clientErr != nil {
				t.Fatalf("client returned unexpected error: %v", clientErr)
			}
			if test.wantErrorCode != 0 && connect.CodeOf(clientErr) != test.wantErrorCode {
				t.Fatalf(
					"client code = %s, want %s (err=%v)",
					connect.CodeOf(clientErr),
					test.wantErrorCode,
					clientErr,
				)
			}
			if got != test.want {
				t.Fatalf("client capability = %s, want %s", got, test.want)
			}
		})
	}
}

func TestHandlerTryCreateClaimRejectsReservedCASCapability(t *testing.T) {
	claims := &capabilityOverrideClaimStore{
		ClaimStore: newClaimsBackend(t),
		capability: storage.CASClaims,
	}
	key := storage.NewClaimKey("prices:cas", "run", "workflow:execution")
	_, err := NewHandlerWithClaims(newTestStore(t), claims).TryCreateClaim(
		context.Background(),
		connect.NewRequest(&temporalessv1.TryCreateClaimRequest{
			Record: &temporalessv1.ClaimRecord{
				SchemaVersion: storage.ClaimRecordSchemaVersion,
				Key:           key.Proto(),
				OwnerId:       "worker",
				ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
				ResourceId:    key.WorkflowID,
			},
		}),
	)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf(
			"code = %s, want %s (err=%v)",
			connect.CodeOf(err),
			connect.CodeFailedPrecondition,
			err,
		)
	}
	if _, found, getErr := claims.GetClaim(context.Background(), key); getErr != nil || found {
		t.Fatalf("claim mutated after reserved CAS rejection: found=%v err=%v", found, getErr)
	}
}

func TestHandlerDeleteClaimRejectsReservedCASCapability(t *testing.T) {
	ctx := context.Background()
	baseClaims := newClaimsBackend(t)
	claims := &capabilityOverrideClaimStore{
		ClaimStore: baseClaims,
		capability: storage.CASClaims,
	}
	key := storage.NewClaimKey("prices:cas", "run", "workflow:execution")
	created, err := baseClaims.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
		SchemaVersion: storage.ClaimRecordSchemaVersion,
		Key:           key.Proto(),
		OwnerId:       "worker",
		ResourceType:  temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_WORKFLOW,
		ResourceId:    key.WorkflowID,
	})
	if err != nil || !created {
		t.Fatalf("seed claim: created=%v err=%v", created, err)
	}

	_, err = NewHandlerWithClaims(newTestStore(t), claims).DeleteClaim(
		ctx,
		connect.NewRequest(&temporalessv1.DeleteClaimRequest{Key: key.Proto()}),
	)
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf(
			"code = %s, want %s (err=%v)",
			connect.CodeOf(err),
			connect.CodeFailedPrecondition,
			err,
		)
	}
	if _, found, getErr := baseClaims.GetClaim(ctx, key); getErr != nil || !found {
		t.Fatalf("claim mutated after reserved CAS rejection: found=%v err=%v", found, getErr)
	}
}

func newTestStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()
	store, _ := newTestStoreAndQuery(t, nil)
	return store
}

func newTestStoreAndQuery(t *testing.T, claims storage.ClaimStore) (*storage.OpenDALStore, storage.QueryStore) {
	t.Helper()

	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	point := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, point, claims)
	if err != nil {
		t.Fatal(err)
	}
	return point, query
}
