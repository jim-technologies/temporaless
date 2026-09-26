package console_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	opendalfs "github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/console"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func at(offset time.Duration) *timestamppb.Timestamp { return timestamppb.New(clockNow.Add(offset)) }

// seedStore writes a small data-engine store: a completed feed, a failed
// feed, and an in-progress feed retrying its third page.
func seedStore(t *testing.T) (string, *opendal.Operator) {
	t.Helper()
	root := t.TempDir()
	operator, err := opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	records := storage.NewOpenDALStore(operator)
	ctx := context.Background()
	input, err := anypb.New(&structpb.Struct{Fields: map[string]*structpb.Value{"pages": structpb.NewNumberValue(6)}})
	if err != nil {
		t.Fatal(err)
	}
	workflow := func(workflowID, runID string, status temporalessv1.WorkflowStatus, created, completed time.Duration) {
		record := &temporalessv1.WorkflowRecord{
			SchemaVersion: storage.WorkflowRecordSchemaVersion,
			Key:           (&storage.WorkflowKey{Namespace: "default", WorkflowID: workflowID, RunID: runID}).Proto(),
			WorkflowType:  "workflow:google.protobuf.Struct->google.protobuf.Struct",
			Input:         input,
			Status:        status,
			CreatedAt:     at(created),
			RunOrderTime:  at(created),
			Annotations:   map[string]string{"feed": strings.TrimPrefix(workflowID, "pull:")},
		}
		if status != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
			record.CompletedAt = at(completed)
		}
		if status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
			record.Failure = &temporalessv1.ActivityFailure{Code: "upstream_5xx", Message: "bad gateway"}
		}
		if err := records.PutWorkflow(ctx, record); err != nil {
			t.Fatal(err)
		}
	}
	workflow("pull:odds", "20260925T075000", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -24*time.Minute, -23*time.Minute)
	workflow("pull:polymarket", "20260925T075500", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, -19*time.Minute, -18*time.Minute)
	workflow("pull:weather", "20260925T080000", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, -14*time.Minute, 0)
	run := storage.WorkflowKey{Namespace: "default", WorkflowID: "pull:weather", RunID: "20260925T080000"}
	activity := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           (&storage.ActivityKey{Namespace: run.Namespace, WorkflowID: run.WorkflowID, RunID: run.RunID, ActivityID: "fetch:page-3"}).Proto(),
		ActivityType:  "activity:google.protobuf.Struct->google.protobuf.Struct",
		Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
		CreatedAt:     at(-10 * time.Minute),
		Attempts: []*temporalessv1.ActivityAttempt{
			{Attempt: 1, StartedAt: at(-11 * time.Minute), CompletedAt: at(-10 * time.Minute), Failure: &temporalessv1.ActivityFailure{Code: "rate_limited", Message: "429"}},
			{Attempt: 2, StartedAt: at(-5 * time.Minute), CompletedAt: at(-4 * time.Minute), Failure: &temporalessv1.ActivityFailure{Code: "rate_limited", Message: "429"}},
		},
		NextAttemptAt: at(30 * time.Second),
		RetryPolicy:   &temporalessv1.RetryPolicy{InitialInterval: durationpb.New(30 * time.Second), BackoffCoefficient: 2, MaximumAttempts: 6},
		RetryTimerId:  "retry:fetch:page-3",
	}
	if err := records.PutActivity(ctx, activity); err != nil {
		t.Fatal(err)
	}
	timer := &temporalessv1.TimerRecord{
		SchemaVersion:   storage.TimerRecordSchemaVersion,
		Key:             (&storage.TimerKey{Namespace: run.Namespace, WorkflowID: run.WorkflowID, RunID: run.RunID, TimerID: "retry:fetch:page-3"}).Proto(),
		TimerKind:       temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY,
		Duration:        durationpb.New(4*time.Minute + 30*time.Second),
		Status:          temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
		FireAt:          at(30 * time.Second),
		CreatedAt:       at(-4 * time.Minute),
		RetryActivityId: "fetch:page-3",
	}
	if err := records.PutTimer(ctx, timer); err != nil {
		t.Fatal(err)
	}
	return root, operator
}

func fingerprint(t *testing.T, root string) map[string][32]byte {
	t.Helper()
	files := map[string][32]byte{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		files[path] = sha256.Sum256(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

type harness struct {
	server *httptest.Server
	root   string
	signer *signer
}

func newHarness(t *testing.T, authenticated bool) *harness {
	t.Helper()
	root, operator := seedStore(t)
	store := &inspection.Store{
		ID:          "engine",
		DisplayName: "Data engine",
		Namespaces:  []string{"default"},
		Records:     storage.NewOpenDALStore(operator),
		Bucket:      inspection.NewOpenDALBucket(operator),
	}
	signer := newECSigner(t, "ec-1")
	var authenticator console.Authenticator
	access := inspection.AllowAll
	var tokens inspection.PageTokens
	if authenticated {
		keys, err := console.NewJWKS("", writeFile(t, "jwks.json", jwks(t, signer)), nil, fixedNow)
		if err != nil {
			t.Fatal(err)
		}
		verifier, err := console.NewJWTVerifier(console.JWTConfig{Issuer: "https://issuer.example.com", Audience: "temporaless-console"}, keys, fixedNow)
		if err != nil {
			t.Fatal(err)
		}
		authenticator = verifier
		// user-7 may read both workspaces, so the outsider token (user-7 in
		// ws-b) is kept out of ws-a's store by the scope check alone.
		access = console.ScopedAccess(console.ScopedAccessConfig{Scopes: map[string]console.StoreScope{"engine": {Workspace: "ws-a"}}},
			authorizerFunc(func(_ context.Context, user, relation, object string) (bool, error) {
				return user == "user:user-7" && relation == "can_read" && (object == "workspace:ws-a" || object == "workspace:ws-b"), nil
			}))
		if tokens, err = console.NewMACPageTokens([]byte(strings.Repeat("s", 32)), time.Hour, fixedNow); err != nil {
			t.Fatal(err)
		}
	}
	service, err := inspection.NewService([]*inspection.Store{store}, inspection.Options{Access: access, PageTokens: tokens, Now: fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	terminal := console.NewTerminalService(service, fixedNow)
	invariantServer, err := console.NewInvariantServer(service, terminal, authenticator)
	if err != nil {
		t.Fatal(err)
	}
	assets := fstest.MapFS{
		"index.html":      {Data: []byte("<!doctype html><title>console</title>")},
		"assets/app-1.js": {Data: []byte("console.log('ok')")},
	}
	handler, err := console.Handler(console.HandlerOptions{API: invariantServer.HTTPHandler(), Authenticator: authenticator, Assets: assets})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &harness{server: server, root: root, signer: signer}
}

func (h *harness) call(t *testing.T, method string, token string, body any) (int, map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, h.server.URL+"/"+method, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := h.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	decoded := map[string]any{}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("decode %s: %v", data, err)
		}
	}
	return response.StatusCode, decoded
}

const (
	inspectionService = "temporaless.v1.RunInspectionService"
)

func terminalService() string {
	for _, method := range console.ProjectedMethods() {
		if strings.HasSuffix(method, ".Get") {
			return strings.TrimSuffix(method, ".Get")
		}
	}
	return ""
}

func TestConsoleAuthenticatesAndScopes(t *testing.T) {
	h := newHarness(t, true)
	before := fingerprint(t, h.root)
	member := h.signer.token(t, "ES256", claims(nil))
	outsider := h.signer.token(t, "ES256", claims(map[string]any{"workspace_id": "ws-b"}))
	nonMember := h.signer.token(t, "ES256", claims(map[string]any{"sub": "user-8"}))
	describe := map[string]any{"store": "engine", "key": map[string]string{"namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000"}}

	tests := []struct {
		name     string
		method   string
		token    string
		body     any
		wantCode int
	}{
		{"missing token", inspectionService + "/GetInspectionCapabilities", "", map[string]any{}, http.StatusUnauthorized},
		{"forged token", inspectionService + "/GetInspectionCapabilities", member + "x", map[string]any{}, http.StatusUnauthorized},
		{"member describes a run", inspectionService + "/DescribeRun", member, describe, http.StatusOK},
		{"other workspace cannot see the store", inspectionService + "/DescribeRun", outsider, describe, http.StatusNotFound},
		{"workspace member without can_read", inspectionService + "/DescribeRun", nonMember, describe, http.StatusNotFound},
		{"facade shares the scope", terminalService() + "/Get", outsider, map[string]any{"source_id": "temporaless.directory", "params": map[string]string{"store": "engine", "namespace": "default"}}, http.StatusNotFound},
		{"request validation", inspectionService + "/DescribeRun", member, map[string]any{"store": "engine"}, http.StatusBadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, body := h.call(t, test.method, test.token, test.body)
			if code != test.wantCode {
				t.Fatalf("status = %d (%v), want %d", code, body, test.wantCode)
			}
		})
	}

	code, capabilities := h.call(t, inspectionService+"/GetInspectionCapabilities", outsider, map[string]any{})
	if code != http.StatusOK || capabilities["stores"] != nil {
		t.Fatalf("outsider capabilities = %d %v, want no stores", code, capabilities)
	}

	// A page token is bound to the caller's scope and filters.
	code, page := h.call(t, inspectionService+"/ListWorkflowDirectory", member, map[string]any{"store": "engine", "namespace": "default", "page_size": 1})
	token, _ := page["nextPageToken"].(string)
	if code != http.StatusOK || token == "" {
		t.Fatalf("first page = %d %v", code, page)
	}
	code, _ = h.call(t, inspectionService+"/ListWorkflowDirectory", member, map[string]any{"store": "engine", "namespace": "default", "page_size": 1, "status": "WORKFLOW_STATUS_FAILED", "page_token": token})
	if code != http.StatusBadRequest {
		t.Fatalf("replayed token with other filters = %d, want 400", code)
	}
	code, _ = h.call(t, inspectionService+"/ListWorkflowDirectory", member, map[string]any{"store": "engine", "namespace": "default", "page_size": 1, "page_token": token})
	if code != http.StatusOK {
		t.Fatalf("second page = %d", code)
	}

	after := fingerprint(t, h.root)
	if len(before) != len(after) {
		t.Fatal("the console changed the store")
	}
	for path, digest := range before {
		if after[path] != digest {
			t.Fatalf("the console changed %s", path)
		}
	}
}

func TestConsoleExposesOnlyReadMethods(t *testing.T) {
	_, operator := seedStore(t)
	store := &inspection.Store{ID: "engine", Namespaces: []string{"default"}, Records: storage.NewOpenDALStore(operator), Bucket: inspection.NewOpenDALBucket(operator)}
	service, err := inspection.NewService([]*inspection.Store{store}, inspection.Options{Access: inspection.AllowAll})
	if err != nil {
		t.Fatal(err)
	}
	terminal := console.NewTerminalService(service, fixedNow)
	server, err := console.NewInvariantServer(service, terminal, nil)
	if err != nil {
		t.Fatal(err)
	}
	var tools []string
	for name := range server.Tools() {
		tools = append(tools, name)
	}
	sort.Strings(tools)
	terminalName := terminalService()
	want := []string{
		terminalName + ".Get",
		terminalName + ".ListSources",
		inspectionService + ".DescribeRun",
		inspectionService + ".GetInspectionCapabilities",
		inspectionService + ".ListNamespaces",
		inspectionService + ".ListScheduledWakes",
		inspectionService + ".ListWorkflowDirectory",
		inspectionService + ".ListWorkflowRuns",
	}
	sort.Strings(want)
	if strings.Join(tools, ",") != strings.Join(want, ",") {
		t.Fatalf("projected tools = %v\nwant %v", tools, want)
	}
	// Besides gRPC reflection, native gRPC serves the inspection service and
	// the terminal facade only.
	var services []string
	for name := range server.GetServiceInfo() {
		if !strings.HasPrefix(name, "grpc.reflection.") {
			services = append(services, name)
		}
	}
	sort.Strings(services)
	if strings.Join(services, ",") != terminalName+","+inspectionService {
		t.Fatalf("native services = %v", services)
	}
	// The facade implements no write: every mutating RPC is Unimplemented.
	if _, err := terminal.SubmitAction(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "Unimplemented") {
		t.Fatalf("SubmitAction = %v, want Unimplemented", err)
	}
	if _, err := terminal.Generate(context.Background(), nil); err == nil {
		t.Fatal("Generate implemented")
	}
}

func TestConsoleHTTPSurface(t *testing.T) {
	h := newHarness(t, false)
	get := func(path string) (int, http.Header, string) {
		response, err := h.server.Client().Get(h.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, response.Header, string(data)
	}
	code, _, body := get("/ui/config")
	if code != http.StatusOK || !strings.Contains(body, `"auth":"loopback"`) || !strings.Contains(body, `"title":"Executions"`) {
		t.Fatalf("/ui/config = %d %s", code, body)
	}
	for _, path := range []string{"/", "/runs/default/pull:weather/20260925T080000"} {
		code, header, body := get(path)
		if code != http.StatusOK || !strings.Contains(body, "<title>console</title>") {
			t.Fatalf("%s = %d %s", path, code, body)
		}
		if !strings.Contains(header.Get("Content-Security-Policy"), "frame-ancestors 'none'") || header.Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("%s is missing security headers: %v", path, header)
		}
	}
	if _, header, _ := get("/assets/app-1.js"); !strings.Contains(header.Get("Cache-Control"), "immutable") {
		t.Fatalf("hashed asset cache = %q", header.Get("Cache-Control"))
	}
	if code, _, _ := get("/healthz"); code != http.StatusOK {
		t.Fatalf("/healthz = %d", code)
	}

	// Loopback mode needs no token.
	code, capabilities := h.call(t, inspectionService+"/GetInspectionCapabilities", "", map[string]any{})
	if code != http.StatusOK || capabilities["stores"] == nil {
		t.Fatalf("loopback capabilities = %d %v", code, capabilities)
	}
}
