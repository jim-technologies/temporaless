package console_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jim-technologies/temporaless/adapters/go/console"
	"github.com/jim-technologies/temporaless/adapters/go/console/internal/terminalv1"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func newTerminal(t *testing.T, access inspection.Access) *console.TerminalService {
	t.Helper()
	_, operator := seedStore(t)
	store := &inspection.Store{ID: "engine", DisplayName: "Data engine", Namespaces: []string{"default"}, Records: storage.NewOpenDALStore(operator), Bucket: inspection.NewOpenDALBucket(operator)}
	service, err := inspection.NewService([]*inspection.Store{store}, inspection.Options{Access: access, Now: fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	return console.NewTerminalService(service, fixedNow)
}

// get calls the facade and returns the response as ProtoJSON, the same
// bytes a dashboard receives.
func get(t *testing.T, terminal *console.TerminalService, source string, params map[string]string) string {
	t.Helper()
	request := dataRequest(source, params)
	response, err := terminal.Get(context.Background(), request)
	if err != nil {
		t.Fatalf("%s: %v", source, err)
	}
	data, err := protojson.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestTerminalSources(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	run := map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000"}
	tests := []struct {
		source string
		params map[string]string
		want   []string
	}{
		{console.SourceStores, nil, []string{`"table"`, `"value":"engine"`, `"label":"Data engine"`, `"payloads":"visible"`}},
		{console.SourceNamespaces, map[string]string{"store": "engine"}, []string{`"value":"default"`}},
		{console.SourceDirectory, map[string]string{"store": "engine", "namespace": "default"}, []string{
			`"records"`, `"status":"retrying"`, `"status":"failed"`, `"status":"completed"`,
			`"pending":"fetch:page-3 · attempt 2/6 · next attempt in 30s · rate_limited"`,
			`"type":"Struct → Struct"`, `"workflow_id":"pull:weather"`, `"runs_page_token":""`,
		}},
		{console.SourceDirectory, map[string]string{"store": "engine", "namespace": "default", "status": "failed"}, []string{`"pending":"upstream_5xx: bad gateway"`}},
		{console.SourceRuns, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather"}, []string{
			`"tableName":"Runs of pull:weather · 1 listed, newest run ID first"`, `"run_id":"20260925T080000"`, `"duration":"14m 00s elapsed"`,
		}},
		{console.SourceWakes, map[string]string{"store": "engine", "namespace": "default"}, []string{
			`"state":"scheduled"`, `"kind":"activity retry"`, `"lateness":"in 30s"`, `"ledger":"agrees"`,
		}},
		{console.SourceRunSummary, run, []string{
			`"objectType":"Workflow run"`, `"status":"Running · retrying"`, `"0 done · 1 retrying · 0 failed"`,
			`"key":"claims"`, `not inspected by this store`, `"relation":"run_of"`,
		}},
		{console.SourceRunPending, run, []string{`"kind":"activity"`, `"detail":"attempt 2 of 6 · last rate_limited: 429 · timer retry:fetch:page-3"`, `"relative":"in 30s"`, `"kind":"timer"`}},
		{console.SourceRunHistory, run, []string{
			`"label":"Workflow started"`, `"label":"fetch:page-3 · attempt 1 failed"`, `"label":"fetch:page-3 · retry backoff after attempt 2"`,
			`"status":"EVENT_STATUS_ERROR"`, `"body":"took 1m 00s · rate_limited: 429"`,
		}},
		{console.SourceRunCompact, run, []string{`"kind":"activity"`, `"status":"retrying"`, `"attempts":2`}},
		{console.SourceRunPayload, run, []string{`"key":"workflow.input"`, `"pages":6`}},
		{console.SourceRunJSON, run, []string{`"key":"workflow"`, `"key":"pending"`, `"RUN_PENDING_REASON_RETRYING"`, `"key":"history"`}},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			body := get(t, terminal, test.source, test.params)
			for _, want := range test.want {
				if !strings.Contains(body, want) {
					t.Fatalf("response lacks %s\n%s", want, body)
				}
			}
		})
	}
}

func TestTerminalWaitsForSelections(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	tests := []struct {
		source string
		params map[string]string
		want   string
	}{
		{console.SourceNamespaces, map[string]string{"store": ""}, `{"table":{}}`},
		{console.SourceDirectory, map[string]string{"store": "engine", "namespace": ""}, `Choose a store and a namespace`},
		{console.SourceRuns, map[string]string{"store": "engine", "namespace": "default", "workflow_id": ""}, `Select a workflow`},
		{console.SourceRunSummary, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": ""}, `Select a run`},
		{console.SourceRunHistory, map[string]string{"store": "engine"}, `{"events":{}}`},
	}
	for _, test := range tests {
		t.Run(test.source, func(t *testing.T) {
			if body := get(t, terminal, test.source, test.params); !strings.Contains(strings.ReplaceAll(body, " ", ""), strings.ReplaceAll(test.want, " ", "")) {
				t.Fatalf("response = %s, want %s", body, test.want)
			}
		})
	}
	_, err := terminal.Get(context.Background(), dataRequest("temporaless.unknown", nil))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown source code = %s", status.Code(err))
	}
	_, err = terminal.Get(context.Background(), dataRequest(console.SourceDirectory, map[string]string{"store": "engine", "namespace": "default", "status": "cancelled"}))
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown status filter code = %s", status.Code(err))
	}
}

func TestTerminalRedactsPayloadsForViewers(t *testing.T) {
	viewer := func(context.Context, *inspection.Store) (inspection.Grant, bool, error) {
		return inspection.Grant{Payloads: false}, true, nil
	}
	terminal := newTerminal(t, viewer)
	body := get(t, terminal, console.SourceRunPayload, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000"})
	if strings.Contains(body, `"pages"`) || !strings.Contains(body, "redacted · ") {
		t.Fatalf("viewer payloads = %s", body)
	}
	if stores := get(t, terminal, console.SourceStores, nil); !strings.Contains(stores, `"payloads":"redacted"`) {
		t.Fatalf("stores = %s", stores)
	}
}

func TestTerminalListSources(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	response, err := terminal.ListSources(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, source := range response.GetSources() {
		ids[source.GetId()] = true
		if !proto.Equal(source, source) || source.GetDescription() == "" {
			t.Fatalf("source %s has no description", source.GetId())
		}
	}
	for _, id := range []string{console.SourceStores, console.SourceNamespaces, console.SourceDirectory, console.SourceRuns, console.SourceWakes,
		console.SourceRunSummary, console.SourceRunPending, console.SourceRunHistory, console.SourceRunCompact, console.SourceRunPayload, console.SourceRunJSON} {
		if !ids[id] {
			t.Fatalf("ListSources lacks %s", id)
		}
	}
}

func dataRequest(source string, params map[string]string) *terminalv1.DataRequest {
	return &terminalv1.DataRequest{SourceId: source, Params: params}
}
