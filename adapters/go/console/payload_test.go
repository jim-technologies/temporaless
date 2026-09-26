package console_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	opendalfs "github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/console"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	orderURL   = "type.googleapis.com/acme.orders.v1.Order"
	invoiceURL = "type.googleapis.com/acme.billing.v1.Invoice"
	opaqueURL  = "type.googleapis.com/temporaless.v1.OpaquePayload"
)

// applicationFile is one application proto file the console binary does not
// link: <pkg>.<message> { string id = 1; int64 quantity = 2; }.
func applicationFile(name, pkg, message string) *descriptorpb.FileDescriptorSet {
	field := func(name string, number int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(number), Type: kind.Enum(),
			Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String(name)}
	}
	return &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String(name), Package: proto.String(pkg), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String(message), Field: []*descriptorpb.FieldDescriptorProto{
			field("id", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING),
			field("quantity", 2, descriptorpb.FieldDescriptorProto_TYPE_INT64),
		}}},
	}}}
}

func orderSet() *descriptorpb.FileDescriptorSet {
	return applicationFile("acme/orders/v1/order.proto", "acme.orders.v1", "Order")
}

func packApplication(t *testing.T, set *descriptorpb.FileDescriptorSet, name, value string) *anypb.Any {
	t.Helper()
	files, err := protodesc.NewFiles(set)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := files.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		t.Fatal(err)
	}
	message := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
	if err := protojson.Unmarshal([]byte(value), message); err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return &anypb.Any{TypeUrl: "type.googleapis.com/" + name, Value: data}
}

// typedHarness serves a store holding one run with application-typed
// payloads through the real console handler: JWT bearer authentication,
// ScopedAccess with payloads reserved for can_write, and the Invariant
// Protocol projections. The order type is registered the way
// cmd/temporaless-console registers a store's payloadDescriptorsFile; the
// invoice type is known to nobody.
func typedHarness(t *testing.T) (*httptest.Server, *signer, *anypb.Any) {
	t.Helper()
	if err := console.RegisterPayloadTypes(orderSet()); err != nil {
		t.Fatal(err)
	}
	renderer, err := inspection.NewPayloadRenderer(orderSet(), 0)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	records := storage.NewOpenDALStore(operator)
	order := packApplication(t, orderSet(), "acme.orders.v1.Order", `{"id":"o-1001","quantity":"3"}`)
	invoice := packApplication(t, applicationFile("acme/billing/v1/invoice.proto", "acme.billing.v1", "Invoice"), "acme.billing.v1.Invoice", `{"id":"inv-77"}`)
	run := storage.WorkflowKey{Namespace: "default", WorkflowID: "app:orders", RunID: "r1"}
	if err := records.PutWorkflow(context.Background(), &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           run.Proto(),
		WorkflowType:  "workflow:acme.orders.v1.Order->acme.billing.v1.Invoice",
		Input:         order,
		Result:        invoice,
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     at(-40 * time.Minute),
		CompletedAt:   at(-39 * time.Minute),
	}); err != nil {
		t.Fatal(err)
	}

	store := &inspection.Store{ID: "engine", Namespaces: []string{"default"}, Records: records, Bucket: inspection.NewOpenDALBucket(operator), Payloads: renderer}
	signer := newECSigner(t, "ec-1")
	keys, err := console.NewJWKS("", writeFile(t, "jwks.json", jwks(t, signer)), nil, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := console.NewJWTVerifier(console.JWTConfig{Issuer: "https://issuer.example.com", Audience: "temporaless-console"}, keys, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	relations := map[string]bool{"user:user-7|can_read": true, "user:user-7|can_write": true, "user:user-9|can_read": true}
	access := console.ScopedAccess(console.ScopedAccessConfig{Scopes: map[string]console.StoreScope{"engine": {Workspace: "ws-a", PayloadRelation: "can_write"}}},
		authorizerFunc(func(_ context.Context, user, relation, object string) (bool, error) {
			return object == "workspace:ws-a" && relations[user+"|"+relation], nil
		}))
	service, err := inspection.NewService([]*inspection.Store{store}, inspection.Options{Access: access, Now: fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	server, err := console.NewInvariantServer(service, console.NewTerminalService(service, fixedNow), verifier)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := console.Handler(console.HandlerOptions{API: server.HTTPHandler(), Authenticator: verifier})
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return httpServer, signer, invoice
}

func postJSON(t *testing.T, server *httptest.Server, path, token string, headers map[string]string, body any) (int, []byte) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}

// describeOverConnect calls DescribeRun through the Connect/HTTP JSON
// projection and returns the decoded response.
func describeOverConnect(t *testing.T, server *httptest.Server, token string, request map[string]any) map[string]any {
	t.Helper()
	code, data := postJSON(t, server, "/"+inspectionService+"/DescribeRun", token, nil, request)
	if code != http.StatusOK {
		t.Fatalf("Connect DescribeRun = %d %s", code, data)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

// describeOverMCP calls the DescribeRun tool through the MCP projection and
// returns the decoded tool output.
func describeOverMCP(t *testing.T, server *httptest.Server, token string, request map[string]any) map[string]any {
	t.Helper()
	code, data := postJSON(t, server, "/mcp", token,
		map[string]string{"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-11-25"},
		map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": map[string]any{
			"name": inspectionService + ".DescribeRun", "arguments": request,
		}})
	if code != http.StatusOK {
		t.Fatalf("MCP DescribeRun = %d %s", code, data)
	}
	var envelope struct {
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Result.IsError || len(envelope.Result.Content) != 1 {
		t.Fatalf("MCP DescribeRun failed: %s", data)
	}
	decoded := map[string]any{}
	if err := json.Unmarshal([]byte(envelope.Result.Content[0].Text), &decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func TestDescribeRunProjectsApplicationPayloads(t *testing.T) {
	server, signer, invoice := typedHarness(t)
	editor := signer.token(t, "ES256", claims(nil))
	viewer := signer.token(t, "ES256", claims(map[string]any{"sub": "user-9"}))
	request := map[string]any{"store": "engine", "key": map[string]string{"namespace": "default", "workflow_id": "app:orders", "run_id": "r1"}}

	registered := map[string]any{"@type": orderURL, "id": "o-1001", "quantity": "3"}
	unknown := map[string]any{"@type": opaqueURL, "typeUrl": invoiceURL, "value": base64.StdEncoding.EncodeToString(invoice.GetValue())}
	redactedOrder := map[string]any{"@type": opaqueURL, "typeUrl": orderURL, "redacted": true}
	redactedInvoice := map[string]any{"@type": opaqueURL, "typeUrl": invoiceURL, "redacted": true}
	surfaces := map[string]func(*testing.T, *httptest.Server, string, map[string]any) map[string]any{
		"connect json": describeOverConnect,
		"mcp":          describeOverMCP,
	}
	tests := []struct {
		name           string
		token          string
		wantVisibility string
		wantInput      map[string]any
		wantResult     map[string]any
		wantRendered   map[string]string
	}{
		{
			name:           "payload reader",
			token:          editor,
			wantVisibility: "PAYLOAD_VISIBILITY_VISIBLE",
			wantInput:      registered,
			wantResult:     unknown,
			wantRendered:   map[string]string{"workflow.input": "json", "workflow.result": "opaque"},
		},
		{
			name:           "redacted viewer",
			token:          viewer,
			wantVisibility: "PAYLOAD_VISIBILITY_REDACTED",
			wantInput:      redactedOrder,
			wantResult:     redactedInvoice,
			wantRendered:   map[string]string{"workflow.input": "redacted", "workflow.result": "redacted"},
		},
	}
	for _, test := range tests {
		for surface, describe := range surfaces {
			t.Run(test.name+" over "+surface, func(t *testing.T) {
				response := describe(t, server, test.token, request)
				if response["payloadVisibility"] != test.wantVisibility {
					t.Fatalf("payloadVisibility = %v, want %s", response["payloadVisibility"], test.wantVisibility)
				}
				workflow, _ := response["workflow"].(map[string]any)
				assertJSON(t, "workflow.input", workflow["input"], test.wantInput)
				assertJSON(t, "workflow.result", workflow["result"], test.wantResult)
				rendered := map[string]string{}
				for _, item := range response["payloads"].([]any) {
					payload := item.(map[string]any)
					switch {
					case payload["redacted"] == true:
						rendered[payload["path"].(string)] = "redacted"
					case payload["json"] != nil:
						rendered[payload["path"].(string)] = "json"
						assertJSON(t, "rendered input", payload["json"], map[string]any{"id": "o-1001", "quantity": "3"})
					case payload["opaque"] != nil:
						rendered[payload["path"].(string)] = "opaque"
					}
				}
				assertJSON(t, "rendered payloads", rendered, test.wantRendered)
			})
		}
	}

	// The terminal facade's DescribeRun JSON source marshals the same
	// response for the dashboard.
	for _, token := range []string{editor, viewer} {
		code, data := postJSON(t, server, "/"+terminalService()+"/Get", token, nil,
			map[string]any{"source_id": console.SourceRunJSON, "params": map[string]string{"store": "engine", "namespace": "default", "workflow_id": "app:orders", "run_id": "r1"}})
		if code != http.StatusOK || !strings.Contains(string(data), opaqueURL) {
			t.Fatalf("run JSON source = %d %s", code, data)
		}
	}
}

func TestRegisterPayloadTypesRejectsConflicts(t *testing.T) {
	if err := console.RegisterPayloadTypes(orderSet()); err != nil {
		t.Fatal(err)
	}
	// The same file again, as a second store would register it, is accepted.
	if err := console.RegisterPayloadTypes(orderSet()); err != nil {
		t.Fatalf("identical re-registration: %v", err)
	}
	// Another definition under the same file name is refused.
	conflicting := applicationFile("acme/orders/v1/order.proto", "acme.orders.v1", "Order")
	conflicting.File[0].MessageType[0].Field[1].Name = proto.String("count")
	if err := console.RegisterPayloadTypes(conflicting); err == nil {
		t.Fatal("a conflicting definition of a registered file was accepted")
	}
	// Linked files keep their generated definitions.
	linked := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{protodesc.ToFileDescriptorProto(anypb.File_google_protobuf_any_proto)}}
	if err := console.RegisterPayloadTypes(linked); err != nil {
		t.Fatalf("linked file: %v", err)
	}
}

func assertJSON(t *testing.T, label string, got, want any) {
	t.Helper()
	gotJSON, _ := json.Marshal(got)
	wantJSON, _ := json.Marshal(want)
	if !bytes.Equal(gotJSON, wantJSON) {
		t.Fatalf("%s = %s, want %s", label, gotJSON, wantJSON)
	}
}
