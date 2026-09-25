package cloudevents_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/jim-technologies/temporaless/adapters/go/cloudevents"
	"github.com/jim-technologies/temporaless/adapters/go/connectstore"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/temporalessv1connect"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPublicationAfterCommit(t *testing.T) {
	tests := []struct {
		name         string
		publishErr   error
		panicPublish bool
		panicID      bool
		id           string
		wantCalls    int32
	}{
		{name: "published", id: "observation-1", wantCalls: 1},
		{name: "publisher unavailable", id: "observation-1", publishErr: errors.New("offline"), wantCalls: 1},
		{name: "publisher panic", id: "observation-1", panicPublish: true, wantCalls: 1},
		{name: "ID factory panic", panicID: true},
		{name: "ID surrounding whitespace", id: " observation-1 "},
		{name: "ID control character", id: "observation-1\u0080"},
		{name: "invalid caller ID", id: "", wantCalls: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(operator.Close)
			store := storage.NewOpenDALStore(operator)
			key := storage.NewWorkflowKey("fetch:prices", "run:1")
			var published atomic.Int32
			interceptor, err := cloudevents.NewInterceptor(cloudevents.Options{
				Source: "urn:example:records", NewID: func() string {
					if test.panicID {
						panic("ID factory unavailable")
					}
					return test.id
				},
				Publish: func(ctx context.Context, observed event.Event) error {
					published.Add(1)
					if _, found, readErr := store.GetWorkflow(ctx, key); readErr != nil || !found {
						t.Errorf("publish ran before commit: found=%v err=%v", found, readErr)
					}
					if observed.Type() != "temporaless.v1.RecordStoreService.PutWorkflow" || observed.ID() != test.id {
						t.Errorf("unexpected metadata: %v", observed.Context)
					}
					if observed.DataContentType() != "application/protobuf" || observed.DataSchema() != "https://type.googleapis.com/temporaless.v1.WorkflowKey" {
						t.Errorf("unexpected payload metadata: %v", observed.Context)
					}
					// Standard structured JSON transport must preserve binary protobuf
					// through the official SDK's data_base64 representation.
					encoded, marshalErr := json.Marshal(observed)
					var decoded event.Event
					if marshalErr != nil || json.Unmarshal(encoded, &decoded) != nil {
						t.Errorf("CloudEvents structured round trip failed: %v", marshalErr)
					}
					var observedKey temporalessv1.WorkflowKey
					if proto.Unmarshal(decoded.Data(), &observedKey) != nil || !proto.Equal(&observedKey, key.Proto()) {
						t.Errorf("wrong typed key: %v", &observedKey)
					}
					if bytes.Contains(observed.Data(), []byte("private-payload")) {
						t.Error("record annotation leaked into event")
					}
					if test.panicPublish {
						panic("publisher unavailable")
					}
					return test.publishErr
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			_, handler := connectstore.NewHTTPHandler(store, connect.WithInterceptors(interceptor))
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			// Installing it on a Go client too must not double-publish.
			client := temporalessv1connect.NewRecordStoreServiceClient(server.Client(), server.URL, connect.WithInterceptors(interceptor))
			requestKey := key.Proto()
			requestKey.ProtoReflect().SetUnknown(protowire.AppendString(protowire.AppendTag(nil, 100, protowire.BytesType), "private-payload"))
			_, err = client.PutWorkflow(context.Background(), connect.NewRequest(&temporalessv1.PutWorkflowRequest{Record: &temporalessv1.WorkflowRecord{
				SchemaVersion: storage.WorkflowRecordSchemaVersion, Key: requestKey,
				WorkflowType: "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
				Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
				Annotations:  map[string]string{"private": "private-payload"},
			}}))
			if err != nil {
				t.Fatalf("committed RPC failed: %v", err)
			}
			if _, found, readErr := store.GetWorkflow(context.Background(), key); readErr != nil || !found {
				t.Fatalf("canonical record missing: found=%v err=%v", found, readErr)
			}
			_, err = client.GetWorkflow(context.Background(), connect.NewRequest(&temporalessv1.GetWorkflowRequest{Key: key.Proto()}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.PutWorkflow(context.Background(), connect.NewRequest(&temporalessv1.PutWorkflowRequest{}))
			if err == nil {
				t.Fatal("invalid storage request succeeded")
			}
			if published.Load() != test.wantCalls {
				t.Fatalf("published %d, want %d", published.Load(), test.wantCalls)
			}
		})
	}
}

func TestDuplicateDeleteObservations(t *testing.T) {
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	var count atomic.Int32
	interceptor, err := cloudevents.NewInterceptor(cloudevents.Options{
		Source: "urn:example:records", NewID: func() string { return fmt.Sprint(count.Add(1)) },
		Publish: func(_ context.Context, observed event.Event) error {
			if observed.Type() != "temporaless.v1.RecordStoreService.DeleteRun" {
				t.Errorf("wrong type: %s", observed.Type())
			}
			var key temporalessv1.WorkflowKey
			if proto.Unmarshal(observed.Data(), &key) != nil || key.GetRunId() != "run:1" {
				t.Errorf("wrong delete key: %v", &key)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, handler := connectstore.NewHTTPHandler(storage.NewOpenDALStore(operator), connect.WithInterceptors(interceptor))
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client := temporalessv1connect.NewRecordStoreServiceClient(server.Client(), server.URL)
	for range 2 {
		_, err := client.DeleteRun(context.Background(), connect.NewRequest(&temporalessv1.DeleteRunRequest{Key: storage.NewWorkflowKey("fetch:prices", "run:1").Proto()}))
		if err != nil {
			t.Fatal(err)
		}
	}
	if count.Load() != 2 {
		t.Fatalf("duplicate invalidations = %d", count.Load())
	}
}

func TestInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		id      func() string
		publish func(context.Context, event.Event) error
	}{
		{name: "relative source", source: "records", id: func() string { return "id" }, publish: func(context.Context, event.Event) error { return nil }},
		{name: "invalid source", source: "urn:records:%zz", id: func() string { return "id" }, publish: func(context.Context, event.Event) error { return nil }},
		{name: "source whitespace", source: "urn:records: ", id: func() string { return "id" }, publish: func(context.Context, event.Event) error { return nil }},
		{name: "missing source", id: func() string { return "id" }, publish: func(context.Context, event.Event) error { return nil }},
		{name: "missing ID factory", source: "urn:example:records", publish: func(context.Context, event.Event) error { return nil }},
		{name: "missing publisher", source: "urn:example:records", id: func() string { return "id" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := cloudevents.NewInterceptor(cloudevents.Options{Source: test.source, NewID: test.id, Publish: test.publish}); err == nil {
				t.Fatal("invalid options accepted")
			}
		})
	}
}
