package console_test

import (
	"context"
	"encoding/json"
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
		{console.SourceRunJSON, run, []string{`{"json":{`, `"workflow":{`, `"RUN_PENDING_REASON_RETRYING"`, `"history":[`, `"workflowId":"pull:weather"`}},
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

// TestTerminalAnswersInDeclaredShapes pins each source to the payload case
// its ListSources shape declares, for a selected run and before a selection,
// and the executions template to render each source with a widget of that
// shape. The DescribeRun JSON source must answer in the json case the json
// widget renders, never an object view of the response.
func TestTerminalAnswersInDeclaredShapes(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	sources, err := terminal.ListSources(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[terminalv1.Shape]string{
		terminalv1.Shape_SHAPE_TABLE:      "table",
		terminalv1.Shape_SHAPE_RECORD_SET: "records",
		terminalv1.Shape_SHAPE_OBJECT:     "object",
		terminalv1.Shape_SHAPE_EVENTS:     "events",
		terminalv1.Shape_SHAPE_JSON:       "json",
	}
	selected := map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000"}
	shapes := map[string]terminalv1.Shape{}
	for _, source := range sources.GetSources() {
		shapes[source.GetId()] = source.GetShape()
		want, ok := cases[source.GetShape()]
		if !ok {
			t.Fatalf("source %s declares shape %s, which no widget in the template renders", source.GetId(), source.GetShape())
		}
		for name, params := range map[string]map[string]string{"selected": selected, "waiting": {"store": "engine"}} {
			response, err := terminal.Get(context.Background(), dataRequest(source.GetId(), params))
			if err != nil {
				t.Fatalf("%s (%s): %v", source.GetId(), name, err)
			}
			payload := response.ProtoReflect()
			field := payload.WhichOneof(payload.Descriptor().Oneofs().ByName("payload"))
			if field == nil || string(field.Name()) != want {
				t.Fatalf("%s (%s) answered %v, want the %s case of %s", source.GetId(), name, field, want, source.GetShape())
			}
		}
	}

	var template struct {
		Widgets []struct {
			ID        string `json:"id"`
			Component string `json:"component"`
			Source    *struct {
				SourceID string `json:"source_id"`
			} `json:"source"`
		} `json:"widgets"`
	}
	if err := json.Unmarshal(console.ExecutionsTemplate(), &template); err != nil {
		t.Fatal(err)
	}
	components := map[string]terminalv1.Shape{
		"select":      terminalv1.Shape_SHAPE_TABLE,
		"table":       terminalv1.Shape_SHAPE_TABLE,
		"record_grid": terminalv1.Shape_SHAPE_RECORD_SET,
		"object_view": terminalv1.Shape_SHAPE_OBJECT,
		"events":      terminalv1.Shape_SHAPE_EVENTS,
		"json":        terminalv1.Shape_SHAPE_JSON,
	}
	for _, widget := range template.Widgets {
		if widget.Source == nil {
			continue
		}
		shape, ok := shapes[widget.Source.SourceID]
		if !ok {
			t.Fatalf("widget %s reads %s, which ListSources does not serve", widget.ID, widget.Source.SourceID)
		}
		if components[widget.Component] != shape {
			t.Fatalf("widget %s renders %s (%s) with a %s widget", widget.ID, widget.Source.SourceID, shape, widget.Component)
		}
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
		{console.SourceDirectory, map[string]string{"store": "engine", "namespace": ""}, `"next_step":"Choose a store and a namespace above."`},
		{console.SourceWakes, map[string]string{"store": "", "namespace": ""}, `"next_step":"Choose a store and a namespace above."`},
		{console.SourceRuns, map[string]string{"store": "engine", "namespace": "default", "workflow_id": ""}, `"next_step":"Pick a workflow in the list above to see its runs."`},
		{console.SourceRunSummary, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": ""}, `Select a run`},
		{console.SourceRunPending, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather"}, `"next_step":"Pick a workflow, then one of its runs."`},
		{console.SourceRunCompact, map[string]string{"store": "engine"}, `"next_step":"Pick a workflow, then one of its runs."`},
		{console.SourceRunHistory, map[string]string{"store": "engine"}, `{"events":{"events":[{"label":"Pick a workflow, then one of its runs."`},
		{console.SourceRunJSON, map[string]string{"store": "engine"}, `{"json":{"next_step":"Pick a workflow, then one of its runs."}}`},
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

func TestTerminalTimeZones(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	run := func(tz string) map[string]string {
		return map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000", "tz": tz}
	}
	directory := func(tz string) map[string]string {
		return map[string]string{"store": "engine", "namespace": "default", "tz": tz}
	}
	// pull:weather started at 08:00:00 UTC; its retry timer fires at
	// 08:14:30 UTC. Kuala Lumpur is UTC+8 all year.
	tests := []struct {
		name   string
		source string
		params map[string]string
		want   []string
	}{
		{"directory defaults to UTC", console.SourceDirectory, directory(""), []string{`"label":"Started (UTC)"`, `"label":"Ordered at (UTC)"`, `"started":"2026-09-25 08:00:00"`}},
		{"directory in a local zone", console.SourceDirectory, directory("Asia/Kuala_Lumpur"), []string{`"label":"Started (Asia/Kuala_Lumpur)"`, `"started":"2026-09-25 16:00:00"`}},
		{"runs", console.SourceRuns, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "tz": "UTC"}, []string{`"label":"Started (UTC)"`, `"label":"Completed (UTC)"`, `"started":"2026-09-25 08:00:00"`}},
		{"wakes", console.SourceWakes, directory("Asia/Kuala_Lumpur"), []string{`"label":"Fires at (Asia/Kuala_Lumpur)"`, `"fires_at":"2026-09-25 16:14:30"`}},
		{"summary", console.SourceRunSummary, run(""), []string{`"label":"Started (UTC)"`, `"value":"2026-09-25 08:00:00"`, `"label":"Observed (UTC)"`}},
		{"pending", console.SourceRunPending, run("Asia/Kuala_Lumpur"), []string{`"label":"At (Asia/Kuala_Lumpur)"`, `"at":"2026-09-25 16:14:30"`}},
		{"history instants carry their offset", console.SourceRunHistory, run(""), []string{`"timestamp":"2026-09-25T08:00:00Z"`, `next attempt 08:14:30 UTC`}},
		{"history in a local zone", console.SourceRunHistory, run("Asia/Kuala_Lumpur"), []string{`"timestamp":"2026-09-25T16:00:00+08:00"`, `next attempt 16:14:30 +08`}},
		{"compact", console.SourceRunCompact, run(""), []string{`"label":"First (UTC)"`, `"label":"Last (UTC)"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := get(t, terminal, test.source, test.params)
			for _, want := range test.want {
				if !strings.Contains(body, want) {
					t.Fatalf("response lacks %s\n%s", want, body)
				}
			}
			for _, format := range []string{"COLUMN_TYPE_TIMESTAMP", "RECORD_FIELD_TYPE_DATETIME", `"format":"datetime"`, `"updatedAt"`} {
				if test.source != console.SourceDirectory && strings.Contains(body, format) {
					t.Fatalf("response still asks the browser to format a time (%s): %s", format, body)
				}
			}
		})
	}
	for _, tz := range []string{"Mars/Olympus_Mons", "Local", "../../etc/passwd", "/etc/localtime", strings.Repeat("A", 80)} {
		_, err := terminal.Get(context.Background(), dataRequest(console.SourceDirectory, directory(tz)))
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("tz %q code = %s, want InvalidArgument", tz, status.Code(err))
		}
	}
}

func TestTerminalRunSummaryByOutcome(t *testing.T) {
	terminal := newTerminal(t, inspection.AllowAll)
	summary := func(workflowID, runID string) string {
		return get(t, terminal, console.SourceRunSummary, map[string]string{"store": "engine", "namespace": "default", "workflow_id": workflowID, "run_id": runID})
	}
	tests := []struct {
		name     string
		body     string
		want     []string
		excluded []string
	}{
		{"a failed run names its failure", summary("pull:polymarket", "20260925T075500"),
			[]string{`"key":"failure","label":"Failure","value":"upstream_5xx: bad gateway"`}, []string{`"label":"Waiting on"`}},
		{"a completed run waits on nothing", summary("pull:odds", "20260925T075000"),
			nil, []string{`"label":"Waiting on"`, `"label":"Failure"`}},
		{"a running run says what it waits on", summary("pull:weather", "20260925T080000"),
			[]string{`"key":"pending","label":"Waiting on","value":"fetch:page-3`}, []string{`"label":"Failure"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := strings.ReplaceAll(test.body, " ", "")
			for _, want := range test.want {
				if !strings.Contains(body, strings.ReplaceAll(want, " ", "")) {
					t.Fatalf("summary lacks %s\n%s", want, test.body)
				}
			}
			for _, excluded := range test.excluded {
				if strings.Contains(body, strings.ReplaceAll(excluded, " ", "")) {
					t.Fatalf("summary has %s\n%s", excluded, test.body)
				}
			}
		})
	}
	// A finished run's pending table says why it is empty.
	pending := get(t, terminal, console.SourceRunPending, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:polymarket", "run_id": "20260925T075500"})
	if !strings.Contains(pending, `"detail":"Nothing pending: the run failed"`) {
		t.Fatalf("failed run pending: %s", pending)
	}
	// Durations read the same everywhere: the retry timer's 4m30s is "4m 30s".
	compact := get(t, terminal, console.SourceRunCompact, map[string]string{"store": "engine", "namespace": "default", "workflow_id": "pull:weather", "run_id": "20260925T080000"})
	if !strings.Contains(compact, `"duration":"4m 30s"`) {
		t.Fatalf("compact timer duration: %s", compact)
	}
}
