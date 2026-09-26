package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	opendalfs "github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func writeTemp(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name: "loopback filesystem store",
			yaml: `
listen: 127.0.0.1:8080
authentication: {loopback: {}}
stores:
  - id: engine
    namespaces: [default]
    filesystem: {root: /var/lib/records}
    overdueGrace: 180s
`,
		},
		{
			name: "jwt with openfga and s3",
			yaml: `
listen: 0.0.0.0:8080
pageTokenKeyFile: /etc/console/page-token-key
authentication:
  jwt: {issuer: https://issuer.example.com, audience: temporaless-console, jwksUrl: https://issuer.example.com/jwks.json}
  openfga: {apiUrl: http://openfga.example.com:8080, storeId: 01STORE}
stores:
  - id: engine
    namespaces: [default]
    workspace: ws-a
    payloadRelation: can_write
    s3: {bucket: state, root: workflows/, accessKeyIdFile: /etc/console/s3-id, secretAccessKeyFile: /etc/console/s3-secret}
`,
		},
		{
			name:    "loopback on a public address",
			yaml:    "listen: 0.0.0.0:8080\nauthentication: {loopback: {}}\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "loopback listen address",
		},
		{
			name:    "loopback with native gRPC",
			yaml:    "listen: 127.0.0.1:8080\ngrpcListen: 127.0.0.1:8081\nauthentication: {loopback: {}}\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "cannot serve native gRPC",
		},
		{
			name:    "jwt without openfga",
			yaml:    "listen: :8080\npageTokenKeyFile: /k\nauthentication: {jwt: {issuer: i, audience: a, jwksFile: /j}}\nstores: [{id: e, workspace: w, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "openfga",
		},
		{
			name:    "jwt store without a workspace",
			yaml:    "listen: :8080\npageTokenKeyFile: /k\nauthentication: {jwt: {issuer: i, audience: a, jwksFile: /j}, openfga: {apiUrl: http://f, storeId: s}}\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "needs a workspace",
		},
		{
			name:    "static token without a page-token key",
			yaml:    "listen: :8080\nauthentication: {staticToken: {tokenFile: /t}}\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "page_token_key_file",
		},
		{
			name:    "unknown field",
			yaml:    "listen: 127.0.0.1:8080\nauthentication: {loopback: {}}\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}, secretAccessKey: inline}]\n",
			wantErr: "unknown field",
		},
		{
			name:    "duplicate store",
			yaml:    "listen: 127.0.0.1:8080\nauthentication: {loopback: {}}\nstores: [{id: e, namespaces: [a], filesystem: {root: /x}}, {id: e, namespaces: [b], filesystem: {root: /y}}]\n",
			wantErr: "unique",
		},
		{
			name:    "no authentication",
			yaml:    "listen: 127.0.0.1:8080\nstores: [{id: e, namespaces: [default], filesystem: {root: /x}}]\n",
			wantErr: "authentication must set",
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := writeTemp(t, dir, string(rune('a'+index))+".yaml", test.yaml)
			_, err := loadConfig(path)
			if test.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err = %v, want %q", err, test.wantErr)
			}
		})
	}
}

// TestDocumentedConfiguration keeps the hosted example in docs/console.md
// loadable: it must parse, validate, and keep its security settings.
func TestDocumentedConfiguration(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "console.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, rest, found := strings.Cut(string(doc), "```yaml\n")
	example, _, closed := strings.Cut(rest, "```")
	if !found || !closed {
		t.Fatal("docs/console.md has no yaml example")
	}
	config, err := loadConfig(writeTemp(t, t.TempDir(), "documented.yaml", example))
	if err != nil {
		t.Fatalf("documented configuration: %v", err)
	}
	store := config.GetStores()[0]
	if config.GetAuthentication().GetJwt() == nil || store.GetWorkspace() == "" || store.GetS3().GetSecretAccessKeyFile() == "" {
		t.Fatalf("documented configuration lost a setting: %v", config)
	}
}

func seedRecords(t *testing.T, root string) {
	t.Helper()
	operator, err := opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	defer operator.Close()
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           (&storage.WorkflowKey{Namespace: "default", WorkflowID: "pull:odds", RunID: "20260925T075000"}).Proto(),
		WorkflowType:  "workflow:google.protobuf.Struct->google.protobuf.Struct",
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		CreatedAt:     timestamppb.New(time.Date(2026, 9, 25, 7, 50, 0, 0, time.UTC)),
		CompletedAt:   timestamppb.New(time.Date(2026, 9, 25, 7, 51, 0, 0, time.UTC)),
	}
	if err := storage.NewOpenDALStore(operator).PutWorkflow(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func TestBuildServesTheConsole(t *testing.T) {
	dir := t.TempDir()
	records := filepath.Join(dir, "records")
	seedRecords(t, records)
	token := "an-operator-token-of-sufficient-length"
	configPath := writeTemp(t, dir, "console.yaml", `
listen: 127.0.0.1:0
title: Data engine executions
pageTokenKeyFile: `+writeTemp(t, dir, "page-key", strings.Repeat("p", 40))+`
authentication:
  staticToken: {tokenFile: `+writeTemp(t, dir, "token", token+"\n")+`}
stores:
  - id: engine
    displayName: Data engine
    namespaces: [default]
    filesystem: {root: `+records+`}
`)
	config, err := loadConfig(configPath)
	if err != nil {
		t.Fatal(err)
	}
	console, err := build(config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer console.close()
	server := httptest.NewServer(console.handler)
	defer server.Close()

	post := func(bearer string) (int, string) {
		request, err := http.NewRequest(http.MethodPost, server.URL+"/temporaless.v1.RunInspectionService/ListWorkflowDirectory",
			bytes.NewReader([]byte(`{"store":"engine","namespace":"default"}`)))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if bearer != "" {
			request.Header.Set("Authorization", "Bearer "+bearer)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}
	if code, _ := post(""); code != http.StatusUnauthorized {
		t.Fatalf("anonymous = %d, want 401", code)
	}
	if code, body := post(token); code != http.StatusOK || !strings.Contains(body, "pull:odds") {
		t.Fatalf("operator = %d %s", code, body)
	}
	response, err := server.Client().Get(server.URL + "/ui/config")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), `"auth":"bearer"`) || !strings.Contains(string(body), `"title":"Data engine executions"`) || strings.Contains(string(body), token) {
		t.Fatalf("/ui/config = %s", body)
	}
	if console.authMode != "static-token" {
		t.Fatalf("auth mode = %s", console.authMode)
	}
}

func TestRunCheck(t *testing.T) {
	dir := t.TempDir()
	records := filepath.Join(dir, "records")
	seedRecords(t, records)
	config := writeTemp(t, dir, "console.yaml", "listen: 127.0.0.1:0\nauthentication: {loopback: {}}\nstores: [{id: engine, namespaces: [default], filesystem: {root: "+records+"}}]\n")
	var stderr bytes.Buffer
	released := false
	release := func(*slog.Logger) { released = true }
	if code := run(context.Background(), []string{"-config", config, "-check"}, &stderr, release); code != 0 || !released {
		t.Fatalf("exit %d (released=%t): %s", code, released, stderr.String())
	}
	if code := run(context.Background(), []string{}, &stderr, release); code != 2 {
		t.Fatalf("missing -config exit %d", code)
	}
}

// ticketFile writes an application file the console binary does not link,
// <pkg>.Ticket { string <fieldName> = 1; }, as a descriptor set.
func ticketFile(t *testing.T, dir, name, pkg, fieldName string) (string, *descriptorpb.FileDescriptorSet) {
	t.Helper()
	set := &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name: proto.String(name), Package: proto.String(pkg), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Ticket"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: proto.String(fieldName), Number: proto.Int32(1), JsonName: proto.String(fieldName),
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
		}}}},
	}}}
	data, err := proto.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return writeTemp(t, dir, filepath.Base(name)+".binpb", string(data)), set
}

func TestBuildRegistersPayloadDescriptors(t *testing.T) {
	dir := t.TempDir()
	records := filepath.Join(dir, "records")
	seedRecords(t, records)
	descriptors, set := ticketFile(t, dir, "acme/cmd/v1/ticket.proto", "acme.cmd.v1", "id")
	files, err := protodesc.NewFiles(set)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := files.FindDescriptorByName("acme.cmd.v1.Ticket")
	if err != nil {
		t.Fatal(err)
	}
	ticket := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
	ticket.Set(descriptor.(protoreflect.MessageDescriptor).Fields().ByName("id"), protoreflect.ValueOfString("t-1"))
	value, err := proto.Marshal(ticket)
	if err != nil {
		t.Fatal(err)
	}
	operator, err := opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": records})
	if err != nil {
		t.Fatal(err)
	}
	defer operator.Close()
	if err := storage.NewOpenDALStore(operator).PutWorkflow(context.Background(), &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           (&storage.WorkflowKey{Namespace: "default", WorkflowID: "app:tickets", RunID: "r1"}).Proto(),
		WorkflowType:  "workflow:acme.cmd.v1.Ticket->acme.cmd.v1.Ticket",
		Input:         &anypb.Any{TypeUrl: "type.googleapis.com/acme.cmd.v1.Ticket", Value: value},
		Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		CreatedAt:     timestamppb.New(time.Date(2026, 9, 25, 7, 50, 0, 0, time.UTC)),
	}); err != nil {
		t.Fatal(err)
	}
	config, err := loadConfig(writeTemp(t, dir, "console.yaml", "listen: 127.0.0.1:0\nauthentication: {loopback: {}}\n"+
		"stores: [{id: engine, namespaces: [default], filesystem: {root: "+records+"}, payloadDescriptorsFile: "+descriptors+"}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	console, err := build(config, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer console.close()
	server := httptest.NewServer(console.handler)
	defer server.Close()
	response, err := server.Client().Post(server.URL+"/temporaless.v1.RunInspectionService/DescribeRun", "application/json",
		strings.NewReader(`{"store":"engine","key":{"namespace":"default","workflow_id":"app:tickets","run_id":"r1"}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	var described struct {
		Workflow struct {
			Input map[string]string `json:"input"`
		} `json:"workflow"`
	}
	if response.StatusCode != http.StatusOK || json.Unmarshal(body, &described) != nil ||
		described.Workflow.Input["@type"] != "type.googleapis.com/acme.cmd.v1.Ticket" || described.Workflow.Input["id"] != "t-1" {
		t.Fatalf("DescribeRun = %d %s", response.StatusCode, body)
	}

	// Payload types are process-wide, so stores that disagree are refused
	// at start instead of rendering with each other's schema.
	first, _ := ticketFile(t, dir, "acme/shared/v1/ticket.proto", "acme.shared.v1", "id")
	second, _ := ticketFile(t, t.TempDir(), "acme/shared/v1/ticket.proto", "acme.shared.v1", "code")
	redeclared, _ := ticketFile(t, t.TempDir(), "acme/cmd/v1/other.proto", "acme.cmd.v1", "id")
	tests := []struct {
		name       string
		a, b       string
		wantReason string
	}{
		{"same file, another definition", first, second, "different definition"},
		{"another file declaring a registered message", descriptors, redeclared, "already declared"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := loadConfig(writeTemp(t, t.TempDir(), "conflict.yaml", "listen: 127.0.0.1:0\nauthentication: {loopback: {}}\nstores:\n"+
				"  - {id: a, namespaces: [default], filesystem: {root: "+records+"}, payloadDescriptorsFile: "+test.a+"}\n"+
				"  - {id: b, namespaces: [default], filesystem: {root: "+records+"}, payloadDescriptorsFile: "+test.b+"}\n"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := build(config, nil, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("build = %v, want an error naming %q", err, test.wantReason)
			}
		})
	}
}
