package connectstore

import (
	"context"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

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
						CodeVersion:   "test-version",
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
						CodeVersion:   "test-version",
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
						CodeVersion:   "test-version",
						Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
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
		CodeVersion:   "test-version",
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

func TestClientStoreListAndDeleteRoundTrip(t *testing.T) {
	ctx := context.Background()
	backend := newTestStore(t)
	_, handler := NewHTTPHandler(backend)
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
			CodeVersion:   "test-version",
			Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		}); err != nil {
			t.Fatal(err)
		}
	}

	records, err := clientStore.ListWorkflows(ctx, "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	if err != nil {
		t.Fatal(err)
	}
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

	records, err = clientStore.ListWorkflows(ctx, "", "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	if err != nil {
		t.Fatal(err)
	}
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
		CodeVersion:   "v1",
		Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
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

	// Event round-trip
	if err := storage.SendEvent(ctx, clientStore, storage.EventKey{
		Namespace:  wfKey.Namespace,
		WorkflowID: wfKey.WorkflowID,
		RunID:      wfKey.RunID,
		EventID:    "approval",
	}, wrapperspb.String("manager")); err != nil {
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
	deleted, err = clientStore.DeleteEvent(ctx, storage.EventKey{
		Namespace:  wfKey.Namespace,
		WorkflowID: wfKey.WorkflowID,
		RunID:      wfKey.RunID,
		EventID:    "approval",
	})
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
		CodeVersion:   "v1",
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

func newClaimsBackend(t *testing.T) storage.ClaimStore {
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

func newTestStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()

	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}
