// Package console is the optional read-only operator console for Temporaless
// record stores. It projects RunInspectionService onto the terminal-core
// TerminalService contract so a generic dashboard template can render
// executions, and it carries the console's authentication, authorization,
// and page-token sealing. Core Temporaless ships no UI; this adapter and
// cmd/temporaless-console are opt-in.
package console

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/console/internal/terminalv1"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TerminalService is the read-only TerminalService facade. It answers Get and
// ListSources only; every write, stream, and AI RPC is the generated
// Unimplemented response.
type TerminalService struct {
	terminalv1.UnimplementedTerminalServiceServer

	inspection inspectionv1.RunInspectionServiceServer
	now        func() time.Time
	// zones caches loaded time zones by name; only valid IANA names are
	// stored, so it holds at most the zone database.
	zones sync.Map
}

var _ terminalv1.TerminalServiceServer = (*TerminalService)(nil)

// NewTerminalService builds the facade over an inspection service. Calls go
// through inspection in-process with the caller's context, so the same
// authorization and scoping apply.
func NewTerminalService(inspection inspectionv1.RunInspectionServiceServer, now func() time.Time) *TerminalService {
	if now == nil {
		now = time.Now
	}
	return &TerminalService{inspection: inspection, now: now}
}

// Source IDs served by the facade.
const (
	SourceStores     = "temporaless.stores"
	SourceNamespaces = "temporaless.namespaces"
	SourceDirectory  = "temporaless.directory"
	SourceRuns       = "temporaless.runs"
	SourceWakes      = "temporaless.wakes"
	SourceRunSummary = "temporaless.run.summary"
	SourceRunPending = "temporaless.run.pending"
	SourceRunHistory = "temporaless.run.history"
	SourceRunCompact = "temporaless.run.compact"
	SourceRunPayload = "temporaless.run.payloads"
	SourceRunJSON    = "temporaless.run.json"
)

// ListSources implements TerminalService.
func (service *TerminalService) ListSources(context.Context, *terminalv1.ListSourcesRequest) (*terminalv1.ListSourcesResponse, error) {
	store := param("store", "Record store ID from temporaless.stores.", true)
	namespace := param("namespace", "Namespace inside the store.", true)
	run := []*terminalv1.SourceParam{
		store, namespace,
		param("workflow_id", "Workflow ID of the run.", true),
		param("run_id", "Run ID to describe.", true),
	}
	tz := param("tz", "Time zone for displayed times: UTC (the default) or an IANA zone name such as Europe/Berlin. Column labels name it.", false)
	timedRun := append(slices.Clone(run), tz)
	pageToken := param("page_token", "Opaque token from the previous page; empty for the first page.", false)
	statusFilter := param("status", "Latest-run status filter.", false)
	statusFilter.Type = terminalv1.ParamType_PARAM_TYPE_ENUM
	statusFilter.EnumValues = []string{"all", "running", "failed", "completed"}
	overdue := param("overdue_only", "Only overdue wakes.", false)
	overdue.Type = terminalv1.ParamType_PARAM_TYPE_BOOLEAN
	tags := []string{"temporaless", "executions", "read-only"}
	return &terminalv1.ListSourcesResponse{Sources: []*terminalv1.Source{
		{Id: SourceStores, Name: "Record stores", Description: "Stores this caller may inspect, as picker choices.", Shape: terminalv1.Shape_SHAPE_TABLE, Tags: tags},
		{Id: SourceNamespaces, Name: "Namespaces", Description: "Namespaces of one store and whether each holds records.", Shape: terminalv1.Shape_SHAPE_TABLE, Params: []*terminalv1.SourceParam{store}, Tags: tags},
		{Id: SourceDirectory, Name: "Workflows", Description: "One row per workflow ID with its latest run and derived pending state.", Shape: terminalv1.Shape_SHAPE_RECORD_SET,
			Params: []*terminalv1.SourceParam{store, namespace, statusFilter, param("prefix", "Workflow ID prefix.", false), pageToken, tz}, Tags: tags},
		{Id: SourceRuns, Name: "Runs of a workflow", Description: "Runs of one workflow ID, newest run ID first.", Shape: terminalv1.Shape_SHAPE_RECORD_SET,
			Params: []*terminalv1.SourceParam{store, namespace, param("workflow_id", "Workflow ID whose runs to list.", true), pageToken, tz}, Tags: tags},
		{Id: SourceWakes, Name: "Scheduled wakes", Description: "SCHEDULED durable timers from the due ledger, with ledger agreement and lateness.", Shape: terminalv1.Shape_SHAPE_RECORD_SET,
			Params: []*terminalv1.SourceParam{store, namespace, overdue, pageToken, tz}, Tags: tags},
		{Id: SourceRunSummary, Name: "Run summary", Description: "Status, derived sub-state, timing, and record counts for one run.", Shape: terminalv1.Shape_SHAPE_OBJECT, Params: timedRun, Tags: tags},
		{Id: SourceRunPending, Name: "Pending", Description: "Retrying activities, scheduled timers, and held claims of one run.", Shape: terminalv1.Shape_SHAPE_TABLE, Params: timedRun, Tags: tags},
		{Id: SourceRunHistory, Name: "History", Description: "History derived from one run's record timestamps; evidence, not a journal.", Shape: terminalv1.Shape_SHAPE_EVENTS, Params: timedRun, Tags: tags},
		{Id: SourceRunCompact, Name: "History by boundary", Description: "One row per activity, timer, event, and claim of one run.", Shape: terminalv1.Shape_SHAPE_TABLE, Params: timedRun, Tags: tags},
		{Id: SourceRunPayload, Name: "Inputs and results", Description: "Rendered workflow, activity, and event payloads of one run.", Shape: terminalv1.Shape_SHAPE_OBJECT, Params: run, Tags: tags},
		{Id: SourceRunJSON, Name: "Run JSON", Description: runJSONDescription, Shape: terminalv1.Shape_SHAPE_JSON, Params: run, Tags: tags},
	}}, nil
}

func param(key, description string, required bool) *terminalv1.SourceParam {
	return &terminalv1.SourceParam{Key: key, Description: description, Required: required, Type: terminalv1.ParamType_PARAM_TYPE_STRING}
}

// Get implements TerminalService.
func (service *TerminalService) Get(ctx context.Context, request *terminalv1.DataRequest) (*terminalv1.DataResponse, error) {
	params := request.GetParams()
	zone, err := service.zone(params["tz"])
	if err != nil {
		return nil, err
	}
	switch request.GetSourceId() {
	case SourceStores:
		return service.stores(ctx)
	case SourceNamespaces:
		return service.namespaces(ctx, params)
	case SourceDirectory:
		return service.directory(ctx, params, zone)
	case SourceRuns:
		return service.runs(ctx, params, zone)
	case SourceWakes:
		return service.wakes(ctx, params, zone)
	case SourceRunSummary, SourceRunPending, SourceRunHistory, SourceRunCompact, SourceRunPayload, SourceRunJSON:
		return service.run(ctx, request.GetSourceId(), params, zone)
	default:
		return nil, status.Errorf(codes.NotFound, "unknown source %q", request.GetSourceId())
	}
}

// unselected reports whether a selection param is still empty. A dashboard
// asks for dependent sources before the operator has picked a store, a
// workflow, or a run; that is a waiting state, answered with an empty
// payload that says what to pick, not an error.
func unselected(params map[string]string, keys ...string) bool {
	for _, key := range keys {
		if strings.TrimSpace(params[key]) == "" {
			return true
		}
	}
	return false
}

// Prompts for sources whose selection is still empty. Generic widgets show
// their own "no data" text for an empty payload, so a waiting source answers
// with one row, event, or object that carries the prompt instead.
const (
	promptStore    = "Choose a store and a namespace above."
	promptWorkflow = "Pick a workflow in the list above to see its runs."
	promptRun      = "Pick a workflow, then one of its runs, to see its durable records, derived history, and pending state."
	// promptRunShort fits a table cell or an event line.
	promptRunShort = "Pick a workflow, then one of its runs."
)

func emptyRecords(tableID, prompt string) *terminalv1.DataResponse {
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Records{Records: &terminalv1.RecordSetPayload{
		TableId:      tableID,
		TableName:    prompt,
		PrimaryField: "next_step",
		Fields:       []*terminalv1.RecordField{textField("next_step", "Next step")},
		Records:      []*terminalv1.WorkRecord{{Id: "next-step", Values: structOf(map[string]any{"next_step": prompt})}},
		Capabilities: &terminalv1.RecordCapabilities{},
	}}}
}

// emptyRun answers a run source before a run is selected.
func emptyRun(sourceID string) *terminalv1.DataResponse {
	switch sourceID {
	case SourceRunPending, SourceRunCompact:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: &terminalv1.TablePayload{
			Columns: []*terminalv1.TableColumn{{Key: "next_step", Label: "Next step"}},
			Rows:    []*structpb.Struct{row(map[string]any{"next_step": promptRunShort})},
		}}}
	case SourceRunHistory:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Events{Events: &terminalv1.EventPayload{
			Events: []*terminalv1.Event{{Label: promptRunShort, Status: terminalv1.EventStatus_EVENT_STATUS_UNSPECIFIED}},
		}}}
	case SourceRunJSON:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Json{Json: structpb.NewStructValue(structOf(map[string]any{"next_step": promptRunShort}))}}
	default:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Object{Object: &terminalv1.ObjectPayload{
			ObjectType:  "Workflow run",
			Title:       "Select a run",
			Description: proto.String(promptRun),
		}}}
	}
}

func (service *TerminalService) stores(ctx context.Context) (*terminalv1.DataResponse, error) {
	capabilities, err := service.inspection.GetInspectionCapabilities(ctx, &inspectionv1.GetInspectionCapabilitiesRequest{})
	if err != nil {
		return nil, err
	}
	table := &terminalv1.TablePayload{Columns: []*terminalv1.TableColumn{
		{Key: "value", Label: "Store"}, {Key: "label", Label: "Name"}, {Key: "payloads", Label: "Payloads"},
	}}
	for _, store := range capabilities.GetStores() {
		payloads := "redacted"
		if store.GetPayloadsVisible() {
			payloads = "visible"
		}
		table.Rows = append(table.Rows, row(map[string]any{"value": store.GetStore(), "label": store.GetDisplayName(), "payloads": payloads}))
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: table}}, nil
}

func (service *TerminalService) namespaces(ctx context.Context, params map[string]string) (*terminalv1.DataResponse, error) {
	if unselected(params, "store") {
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: &terminalv1.TablePayload{}}}, nil
	}
	response, err := service.inspection.ListNamespaces(ctx, &inspectionv1.ListNamespacesRequest{Store: params["store"]})
	if err != nil {
		return nil, err
	}
	table := &terminalv1.TablePayload{Columns: []*terminalv1.TableColumn{{Key: "value", Label: "Namespace"}, {Key: "label", Label: "Label"}}}
	for _, namespace := range response.GetNamespaces() {
		label := namespace.GetNamespace()
		if !namespace.GetPresent() {
			label += " (no runs yet)"
		}
		table.Rows = append(table.Rows, row(map[string]any{"value": namespace.GetNamespace(), "label": label}))
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: table}}, nil
}

var workflowStatusFilters = map[string]temporalessv1.WorkflowStatus{
	"":          temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED,
	"all":       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED,
	"running":   temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
	"failed":    temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED,
	"completed": temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
}

func (service *TerminalService) directory(ctx context.Context, params map[string]string, zone displayZone) (*terminalv1.DataResponse, error) {
	if unselected(params, "store", "namespace") {
		return emptyRecords(SourceDirectory, promptStore), nil
	}
	filter, ok := workflowStatusFilters[params["status"]]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "status must be all, running, failed, or completed")
	}
	response, err := service.inspection.ListWorkflowDirectory(ctx, &inspectionv1.ListWorkflowDirectoryRequest{
		Store:            params["store"],
		Namespace:        params["namespace"],
		Status:           filter,
		WorkflowIdPrefix: params["prefix"],
		PageToken:        params["page_token"],
	})
	if err != nil {
		return nil, err
	}
	now := service.now()
	records := &terminalv1.RecordSetPayload{
		TableId:      SourceDirectory,
		TableName:    "Workflows",
		PrimaryField: "workflow_id",
		Fields: []*terminalv1.RecordField{
			stateField("status", "Status"),
			textField("workflow_id", "Workflow ID"),
			textField("run_id", "Latest run"),
			textField("type", "Type"),
			textField("ordered_at", zone.label("Ordered at")),
			textField("started", zone.label("Started")),
			textField("duration", "Duration"),
			textField("pending", "Waiting on or failure"),
		},
		Capabilities:  &terminalv1.RecordCapabilities{},
		NextPageToken: optional(response.GetNextPageToken()),
	}
	for _, entry := range response.GetEntries() {
		pointer := entry.GetPointer()
		run := entry.GetRun()
		key := pointer.GetKey()
		if run != nil {
			key = run.GetKey()
		}
		state, hint := runState(run.GetStatus(), entry.GetPending(), run.GetFailure(), now)
		if run == nil {
			state, hint = "unknown", "the latest-run pointer names a run whose workflow record is missing"
		} else if entry.GetStalePointer() {
			hint = strings.TrimPrefix(hint+" · pointer lags the record", " · ")
		}
		records.Records = append(records.Records, &terminalv1.WorkRecord{
			Id: key.GetWorkflowId(),
			Values: structOf(map[string]any{
				"status":      state,
				"workflow_id": key.GetWorkflowId(),
				"run_id":      key.GetRunId(),
				"type":        shortType(run.GetWorkflowType()),
				"ordered_at":  zone.cell(pointer.GetRunOrderTime()),
				"started":     zone.cell(run.GetCreatedAt()),
				"duration":    runDuration(run.GetCreatedAt(), run.GetCompletedAt(), now),
				"pending":     hint,
			}),
			UpdatedAt: optional(timestamp(pointer.GetRecordTime())),
			Context: map[string]string{
				"namespace":       key.GetNamespace(),
				"workflow_id":     key.GetWorkflowId(),
				"run_id":          key.GetRunId(),
				"runs_page_token": "",
			},
		})
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Records{Records: records}}, nil
}

func (service *TerminalService) runs(ctx context.Context, params map[string]string, zone displayZone) (*terminalv1.DataResponse, error) {
	if unselected(params, "store", "namespace", "workflow_id") {
		return emptyRecords(SourceRuns, promptWorkflow), nil
	}
	response, err := service.inspection.ListWorkflowRuns(ctx, &inspectionv1.ListWorkflowRunsRequest{
		Store:      params["store"],
		Namespace:  params["namespace"],
		WorkflowId: params["workflow_id"],
		PageToken:  params["page_token"],
	})
	if err != nil {
		return nil, err
	}
	now := service.now()
	name := fmt.Sprintf("Runs of %s · %d listed, newest run ID first", params["workflow_id"], response.GetTotalListed())
	if missing := response.GetRunsWithoutWorkflowRecord(); missing > 0 {
		name += fmt.Sprintf(" · %d without a workflow record", missing)
	}
	records := &terminalv1.RecordSetPayload{
		TableId:      SourceRuns,
		TableName:    name,
		PrimaryField: "run_id",
		Fields: []*terminalv1.RecordField{
			stateField("status", "Status"),
			textField("run_id", "Run ID"),
			textField("started", zone.label("Started")),
			textField("completed", zone.label("Completed")),
			textField("duration", "Duration"),
			textField("failure", "Failure"),
		},
		Capabilities:  &terminalv1.RecordCapabilities{},
		NextPageToken: optional(response.GetNextPageToken()),
		Total:         proto.Int64(int64(response.GetTotalListed())),
	}
	for _, run := range response.GetRuns() {
		state := map[temporalessv1.WorkflowStatus]string{
			temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:   "completed",
			temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:      "failed",
			temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS: "running",
		}[run.GetStatus()]
		records.Records = append(records.Records, &terminalv1.WorkRecord{
			Id: run.GetKey().GetRunId(),
			Values: structOf(map[string]any{
				"status":    state,
				"run_id":    run.GetKey().GetRunId(),
				"started":   zone.cell(run.GetCreatedAt()),
				"completed": zone.cell(run.GetCompletedAt()),
				"duration":  runDuration(run.GetCreatedAt(), run.GetCompletedAt(), now),
				"failure":   failureText(run.GetFailure()),
			}),
			Context: map[string]string{"run_id": run.GetKey().GetRunId()},
		})
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Records{Records: records}}, nil
}

func (service *TerminalService) wakes(ctx context.Context, params map[string]string, zone displayZone) (*terminalv1.DataResponse, error) {
	if unselected(params, "store", "namespace") {
		return emptyRecords(SourceWakes, promptStore), nil
	}
	overdueOnly, err := strconv.ParseBool(defaultString(params["overdue_only"], "false"))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "overdue_only must be true or false")
	}
	response, err := service.inspection.ListScheduledWakes(ctx, &inspectionv1.ListScheduledWakesRequest{
		Store:       params["store"],
		Namespace:   params["namespace"],
		OverdueOnly: overdueOnly,
		PageToken:   params["page_token"],
	})
	if err != nil {
		return nil, err
	}
	now := service.now()
	name := "Scheduled wakes"
	var notes []string
	if count := response.GetQuarantinedEntries(); count > 0 {
		notes = append(notes, fmt.Sprintf("%d quarantined ledger entries", count))
	}
	if count := response.GetInvalidEntries(); count > 0 {
		notes = append(notes, fmt.Sprintf("%d invalid ledger entries on this page", count))
	}
	if len(notes) > 0 {
		name += " · " + strings.Join(notes, " · ")
	}
	records := &terminalv1.RecordSetPayload{
		TableId:      SourceWakes,
		TableName:    name,
		PrimaryField: "timer_id",
		Fields: []*terminalv1.RecordField{
			{Key: "state", Label: "State", Type: terminalv1.RecordFieldType_RECORD_FIELD_TYPE_SINGLE_SELECT, ReadOnly: true, Choices: []*terminalv1.RecordChoice{
				{Value: "overdue", Label: "Overdue", Color: proto.String("warn")},
				{Value: "scheduled", Label: "Scheduled", Color: proto.String("info")},
				{Value: "run_finished", Label: "Run finished", Color: proto.String("neutral")},
			}},
			textField("kind", "Kind"),
			textField("fires_at", zone.label("Fires at")),
			textField("lateness", "Relative"),
			textField("workflow_id", "Workflow ID"),
			textField("run_id", "Run ID"),
			textField("timer_id", "Timer ID"),
			textField("ledger", "Ledger"),
		},
		Capabilities:  &terminalv1.RecordCapabilities{},
		NextPageToken: optional(response.GetNextPageToken()),
	}
	for _, wake := range response.GetWakes() {
		timer := wake.GetTimer()
		state := "scheduled"
		switch {
		case wake.GetWorkflowStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			wake.GetWorkflowStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			state = "run_finished"
		case wake.GetOverdue():
			state = "overdue"
		}
		records.Records = append(records.Records, &terminalv1.WorkRecord{
			Id: fmt.Sprintf("%s/%s/%s", wake.GetWorkflowKey().GetWorkflowId(), wake.GetWorkflowKey().GetRunId(), timer.GetKey().GetTimerId()),
			Values: structOf(map[string]any{
				"state":       state,
				"kind":        timerKind(timer.GetTimerKind()),
				"fires_at":    zone.cell(timer.GetFireAt()),
				"lateness":    relative(timer.GetFireAt().AsTime(), now),
				"workflow_id": wake.GetWorkflowKey().GetWorkflowId(),
				"run_id":      wake.GetWorkflowKey().GetRunId(),
				"timer_id":    timer.GetKey().GetTimerId(),
				"ledger":      ledgerText(wake.GetLedgerState()),
			}),
			Context: map[string]string{
				"workflow_id": wake.GetWorkflowKey().GetWorkflowId(),
				"run_id":      wake.GetWorkflowKey().GetRunId(),
			},
		})
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Records{Records: records}}, nil
}

func (service *TerminalService) run(ctx context.Context, sourceID string, params map[string]string, zone displayZone) (*terminalv1.DataResponse, error) {
	if unselected(params, "store", "namespace", "workflow_id", "run_id") {
		return emptyRun(sourceID), nil
	}
	description, err := service.inspection.DescribeRun(ctx, &inspectionv1.DescribeRunRequest{
		Store: params["store"],
		Key: &temporalessv1.WorkflowKey{
			Namespace:  params["namespace"],
			WorkflowId: params["workflow_id"],
			RunId:      params["run_id"],
		},
	})
	if err != nil {
		return nil, err
	}
	switch sourceID {
	case SourceRunSummary:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Object{Object: runSummaryObject(params, description, zone)}}, nil
	case SourceRunPending:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: pendingTable(description, zone)}}, nil
	case SourceRunHistory:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Events{Events: historyEvents(description, zone)}}, nil
	case SourceRunCompact:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Table{Table: compactTable(description, zone)}}, nil
	case SourceRunPayload:
		return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Object{Object: payloadsObject(description)}}, nil
	default:
		return runJSON(description)
	}
}

// runJSONDescription says what the DescribeRun JSON source holds: records as
// stored except for OpaquePayload stand-ins, plus derived views.
const runJSONDescription = "temporaless.v1.RunInspectionService/DescribeRun as ProtoJSON. Records are as stored, except that " +
	"a payload whose type this server cannot resolve, and every payload when yours are redacted, is a " +
	"temporaless.v1.OpaquePayload naming the stored type. The history and pending state are derived."

// runJSON returns the complete DescribeRun response as one ProtoJSON document
// in the terminal contract's json case, which the json widget renders
// verbatim. ProtoJSON keeps 64-bit integers as strings, so nothing loses
// precision on the way through google.protobuf.Value.
func runJSON(description *inspectionv1.DescribeRunResponse) (*terminalv1.DataResponse, error) {
	data, err := protojson.Marshal(description)
	if err != nil {
		return nil, err
	}
	document := &structpb.Value{}
	if err := protojson.Unmarshal(data, document); err != nil {
		return nil, err
	}
	return &terminalv1.DataResponse{Payload: &terminalv1.DataResponse_Json{Json: document}}, nil
}

func runID(description *inspectionv1.DescribeRunResponse) string {
	key := description.GetWorkflow().GetKey()
	return key.GetWorkflowId() + "/" + key.GetRunId()
}

var stateChoices = []*terminalv1.RecordChoice{
	{Value: "completed", Label: "Completed", Color: proto.String("ok")},
	{Value: "failed", Label: "Failed", Color: proto.String("danger")},
	{Value: "running", Label: "Running", Color: proto.String("info")},
	{Value: "overdue_wake", Label: "Overdue wake", Color: proto.String("warn")},
	{Value: "retrying", Label: "Retrying", Color: proto.String("warn")},
	{Value: "stale_claim", Label: "Stale claim", Color: proto.String("warn")},
	{Value: "executing", Label: "Executing", Color: proto.String("info")},
	{Value: "sleeping", Label: "Sleeping", Color: proto.String("neutral")},
	{Value: "polling", Label: "Polling", Color: proto.String("neutral")},
	{Value: "waiting", Label: "Waiting, no wake", Color: proto.String("neutral")},
	{Value: "unknown", Label: "No workflow record", Color: proto.String("neutral")},
}

func stateField(key, label string) *terminalv1.RecordField {
	return &terminalv1.RecordField{Key: key, Label: label, Type: terminalv1.RecordFieldType_RECORD_FIELD_TYPE_SINGLE_SELECT, ReadOnly: true, Choices: stateChoices}
}

func textField(key, label string) *terminalv1.RecordField {
	return &terminalv1.RecordField{Key: key, Label: label, Type: terminalv1.RecordFieldType_RECORD_FIELD_TYPE_TEXT, ReadOnly: true}
}

// runState maps recorded status plus derived pending state onto one status
// value and a one-line hint that names the evidencing record. It never
// reports a status the records cannot evidence.
func runState(workflowStatus temporalessv1.WorkflowStatus, pending *inspectionv1.RunPendingState, failure *temporalessv1.ActivityFailure, now time.Time) (string, string) {
	switch workflowStatus {
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
		return "completed", ""
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		return "failed", failureText(failure)
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS:
	default:
		return "unknown", ""
	}
	at := pending.GetAt()
	switch pending.GetReason() {
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_OVERDUE_WAKE:
		return "overdue_wake", fmt.Sprintf("timer %s is %s", pending.GetResourceId(), relative(at.AsTime(), now))
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_RETRYING:
		hint := fmt.Sprintf("%s · attempt %d", pending.GetResourceId(), pending.GetAttempt())
		if pending.GetMaximumAttempts() > 0 {
			hint += fmt.Sprintf("/%d", pending.GetMaximumAttempts())
		}
		if at != nil {
			hint += " · next attempt " + relative(at.AsTime(), now)
		}
		if code := pending.GetLastFailure().GetCode(); code != "" {
			hint += " · " + code
		}
		return "retrying", hint
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_STALE_CLAIM:
		return "stale_claim", fmt.Sprintf("claim %s lease expired, %s; verify before cleanup", pending.GetResourceId(), relative(at.AsTime(), now))
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_EXECUTING:
		return "executing", fmt.Sprintf("claim %s held, lease ends %s", pending.GetResourceId(), relativeOrEmpty(at, now))
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_SLEEPING:
		return "sleeping", fmt.Sprintf("wakes %s · %s", relative(at.AsTime(), now), pending.GetResourceId())
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_POLLING:
		return "polling", fmt.Sprintf("polls %s · %s", relative(at.AsTime(), now), pending.GetResourceId())
	case inspectionv1.RunPendingReason_RUN_PENDING_REASON_WAITING_NO_WAKE:
		return "waiting", "no durable wake, retry, or claim will re-invoke this run"
	default:
		return "running", ""
	}
}

func runSummaryObject(params map[string]string, description *inspectionv1.DescribeRunResponse, zone displayZone) *terminalv1.ObjectPayload {
	workflow := description.GetWorkflow()
	observed := description.GetObservedAt().AsTime()
	state, hint := runState(workflow.GetStatus(), description.GetPending(), workflow.GetFailure(), observed)
	label := map[string]string{}
	for _, choice := range stateChoices {
		label[choice.GetValue()] = choice.GetLabel()
	}
	statusText := label[state]
	if workflow.GetStatus() == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS && state != "running" {
		statusText = "Running · " + strings.ToLower(label[state])
	}

	var done, retrying, failed int
	for _, activity := range description.GetActivities() {
		switch activity.GetStatus() {
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED:
			done++
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING:
			retrying++
		case temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
			failed++
		}
	}
	var scheduledTimers, firedTimers int
	for _, timer := range description.GetTimers() {
		switch timer.GetStatus() {
		case temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED:
			scheduledTimers++
		case temporalessv1.TimerStatus_TIMER_STATUS_FIRED:
			firedTimers++
		}
	}
	claims := fmt.Sprintf("%d held", len(description.GetClaims()))
	if !description.GetClaimsInspected() {
		claims = "not inspected by this store"
	}

	// The line under the status says why the run is where it is: what an
	// unfinished run waits on, or how a failed run failed.
	properties := []*terminalv1.ObjectProperty{prop("status", "Status", statusText, "Summary")}
	switch workflow.GetStatus() {
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS:
		properties = append(properties, prop("pending", "Waiting on", defaultString(hint, "—"), "Summary"))
	case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		properties = append(properties, prop("failure", "Failure", defaultString(failureText(workflow.GetFailure()), "no failure recorded"), "Summary"))
	}
	properties = append(properties,
		prop("workflow_id", "Workflow ID", params["workflow_id"], "Summary"),
		prop("run_id", "Run ID", params["run_id"], "Summary"),
		prop("type", "Type", defaultString(workflow.GetWorkflowType(), "—"), "Summary"),
		prop("namespace", "Namespace", params["namespace"], "Summary"),
		prop("store", "Store", params["store"], "Summary"),
		timeProp("started", "Started", workflow.GetCreatedAt(), "Timing", zone),
		timeProp("ordered_at", "Ordered at", workflow.GetRunOrderTime(), "Timing", zone),
		timeProp("completed", "Completed", workflow.GetCompletedAt(), "Timing", zone),
		prop("duration", "Duration", runDuration(workflow.GetCreatedAt(), workflow.GetCompletedAt(), observed), "Timing"),
		timeProp("observed_at", "Observed", description.GetObservedAt(), "Timing", zone),
		prop("activities", "Activities", fmt.Sprintf("%d done · %d retrying · %d failed", done, retrying, failed), "Evidence"),
		prop("timers", "Timers", fmt.Sprintf("%d scheduled · %d fired", scheduledTimers, firedTimers), "Evidence"),
		prop("events", "Events", fmt.Sprintf("%d received", len(description.GetEvents())), "Evidence"),
		prop("claims", "Claims", claims, "Evidence"),
	)
	if description.GetTruncated() {
		properties = append(properties, prop("truncated", "Truncated", "a record kind exceeded the store's per-run bound; counts are partial", "Evidence"))
	}
	keys := make([]string, 0, len(workflow.GetAnnotations()))
	for key := range workflow.GetAnnotations() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		properties = append(properties, prop("annotation."+key, key, workflow.GetAnnotations()[key], "Annotations"))
	}
	tags := []string{"read-only", "derived history"}
	if description.GetPayloadVisibility() == inspectionv1.PayloadVisibility_PAYLOAD_VISIBILITY_REDACTED {
		tags = append(tags, "payloads redacted")
	}
	object := &terminalv1.ObjectPayload{
		ObjectType: "Workflow run",
		ObjectId:   params["workflow_id"] + "/" + params["run_id"],
		Title:      params["workflow_id"],
		Description: proto.String("Derived from durable records. Temporaless keeps point records, not a journal: " +
			"overwritten intermediate states and released claims are not shown."),
		Status:     proto.String(statusText),
		Tags:       tags,
		Properties: properties,
		Links: []*terminalv1.ObjectLink{{
			Relation:   "run_of",
			TargetType: "Workflow",
			TargetId:   params["workflow_id"],
			Label:      "All runs of " + params["workflow_id"],
			Context:    map[string]string{"workflow_id": params["workflow_id"], "runs_page_token": ""},
		}},
	}
	if workflow == nil {
		object.Status = proto.String("No workflow record")
	}
	return object
}

func pendingTable(description *inspectionv1.DescribeRunResponse, zone displayZone) *terminalv1.TablePayload {
	observed := description.GetObservedAt().AsTime()
	table := &terminalv1.TablePayload{Columns: []*terminalv1.TableColumn{
		{Key: "kind", Label: "Kind"},
		{Key: "id", Label: "ID"},
		{Key: "state", Label: "State"},
		{Key: "relative", Label: "When"},
		{Key: "at", Label: zone.label("At")},
		{Key: "detail", Label: "Detail"},
	}}
	for _, activity := range description.GetActivities() {
		if activity.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
			continue
		}
		attempts := activity.GetAttempts()
		detail := fmt.Sprintf("attempt %d", len(attempts))
		if limit := activity.GetRetryPolicy().GetMaximumAttempts(); limit > 0 {
			detail += fmt.Sprintf(" of %d", limit)
		}
		if len(attempts) > 0 {
			if failure := failureText(attempts[len(attempts)-1].GetFailure()); failure != "" {
				detail += " · last " + failure
			}
		}
		if activity.GetRetryTimerId() != "" {
			detail += " · timer " + activity.GetRetryTimerId()
		}
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "activity", "id": activity.GetKey().GetActivityId(), "state": "retrying",
			"at": zone.cell(activity.GetNextAttemptAt()), "relative": relativeOrEmpty(activity.GetNextAttemptAt(), observed), "detail": detail,
		}))
	}
	for _, timer := range description.GetTimers() {
		if timer.GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED {
			continue
		}
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "timer", "id": timer.GetKey().GetTimerId(), "state": "scheduled",
			"at": zone.cell(timer.GetFireAt()), "relative": relativeOrEmpty(timer.GetFireAt(), observed),
			"detail": timerKind(timer.GetTimerKind()) + " · woken by the timer scanner (at least once)",
		}))
	}
	for _, claim := range description.GetClaims() {
		state := "held"
		if claim.GetLeaseExpiresAt() != nil && claim.GetLeaseExpiresAt().AsTime().Before(observed) {
			state = "stale"
		}
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "claim", "id": claim.GetKey().GetClaimId(), "state": state,
			"at": zone.cell(claim.GetLeaseExpiresAt()), "relative": relativeOrEmpty(claim.GetLeaseExpiresAt(), observed),
			"detail": "owner " + claim.GetOwnerId() + " · lease expiry is diagnostic",
		}))
	}
	if len(table.Rows) == 0 {
		reason := "no retrying activity, scheduled timer, or held claim"
		switch description.GetWorkflow().GetStatus() {
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
			reason = "the run completed"
		case temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			reason = "the run failed"
		}
		table.Rows = append(table.Rows, row(map[string]any{"kind": "—", "id": "", "state": "", "relative": "", "at": "", "detail": "Nothing pending: " + reason}))
	}
	return table
}

var historyText = map[inspectionv1.RunHistoryEventKind]struct {
	label  string
	status terminalv1.EventStatus
}{
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_STARTED:           {"Workflow started", terminalv1.EventStatus_EVENT_STATUS_INFO},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_COMPLETED:         {"Workflow completed", terminalv1.EventStatus_EVENT_STATUS_OK},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_WORKFLOW_FAILED:            {"Workflow failed", terminalv1.EventStatus_EVENT_STATUS_ERROR},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_SUCCEEDED: {"attempt %d succeeded", terminalv1.EventStatus_EVENT_STATUS_OK},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_FAILED:    {"attempt %d failed", terminalv1.EventStatus_EVENT_STATUS_ERROR},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_ATTEMPT_OPEN:      {"attempt %d started, no completion recorded", terminalv1.EventStatus_EVENT_STATUS_PENDING},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF:     {"retry backoff after attempt %d", terminalv1.EventStatus_EVENT_STATUS_WARN},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_COMPLETED:         {"activity completed", terminalv1.EventStatus_EVENT_STATUS_OK},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_FAILED:            {"activity failed", terminalv1.EventStatus_EVENT_STATUS_ERROR},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED:            {"timer scheduled", terminalv1.EventStatus_EVENT_STATUS_PENDING},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_FIRED:                {"timer fired", terminalv1.EventStatus_EVENT_STATUS_OK},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_EVENT_RECEIVED:             {"event received", terminalv1.EventStatus_EVENT_STATUS_INFO},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_CLAIM_HELD:                 {"claim held", terminalv1.EventStatus_EVENT_STATUS_INFO},
	inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_UNSPECIFIED:                {"record", terminalv1.EventStatus_EVENT_STATUS_UNSPECIFIED},
}

func historyEvents(description *inspectionv1.DescribeRunResponse, zone displayZone) *terminalv1.EventPayload {
	payload := &terminalv1.EventPayload{}
	for _, event := range description.GetHistory() {
		text := historyText[event.GetKind()]
		label := text.label
		if strings.Contains(label, "%d") {
			label = fmt.Sprintf(label, event.GetAttempt())
		}
		if resource := event.GetResourceId(); resource != "" {
			label = resource + " · " + label
		}
		var body []string
		if event.GetUntil() != nil {
			switch event.GetKind() {
			case inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_TIMER_SCHEDULED:
				body = append(body, fmt.Sprintf("%s timer fires %s", timerKind(event.GetTimerKind()), zone.clock(event.GetUntil())))
			case inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_ACTIVITY_RETRY_BACKOFF:
				body = append(body, "next attempt "+zone.clock(event.GetUntil()))
			case inspectionv1.RunHistoryEventKind_RUN_HISTORY_EVENT_KIND_CLAIM_HELD:
				body = append(body, "diagnostic lease until "+zone.clock(event.GetUntil()))
			default:
				body = append(body, "took "+span(event.GetTime(), event.GetUntil()))
			}
		}
		if failure := failureText(event.GetFailure()); failure != "" {
			body = append(body, failure)
		}
		source := "workflow"
		if event.GetResourceId() != "" {
			source = strings.SplitN(event.GetEventId(), "/", 2)[0]
		}
		payload.Events = append(payload.Events, &terminalv1.Event{
			Timestamp: zone.instant(event.GetTime()),
			Label:     label,
			Status:    text.status,
			Body:      optional(strings.Join(body, " · ")),
			Source:    proto.String(source),
			Tags:      []string{strings.ToLower(strings.TrimPrefix(event.GetKind().String(), "RUN_HISTORY_EVENT_KIND_"))},
		})
	}
	return payload
}

func compactTable(description *inspectionv1.DescribeRunResponse, zone displayZone) *terminalv1.TablePayload {
	observed := description.GetObservedAt().AsTime()
	table := &terminalv1.TablePayload{Columns: []*terminalv1.TableColumn{
		{Key: "kind", Label: "Kind"},
		{Key: "id", Label: "ID"},
		{Key: "status", Label: "Status"},
		{Key: "attempts", Label: "Attempts", Type: terminalv1.ColumnType_COLUMN_TYPE_NUMBER},
		{Key: "first", Label: zone.label("First")},
		{Key: "last", Label: zone.label("Last")},
		{Key: "duration", Label: "Duration"},
		{Key: "detail", Label: "Detail"},
	}}
	for _, activity := range description.GetActivities() {
		attempts := activity.GetAttempts()
		var first, last *timestamppb.Timestamp
		if len(attempts) > 0 {
			first = attempts[0].GetStartedAt()
			last = attempts[len(attempts)-1].GetCompletedAt()
		}
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "activity", "id": activity.GetKey().GetActivityId(),
			"status":   strings.ToLower(strings.TrimPrefix(activity.GetStatus().String(), "ACTIVITY_STATUS_")),
			"attempts": len(attempts), "first": zone.cell(first), "last": zone.cell(last),
			"duration": runDuration(first, last, observed), "detail": failureText(activity.GetFailure()),
		}))
	}
	for _, timer := range description.GetTimers() {
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "timer", "id": timer.GetKey().GetTimerId(),
			"status":   strings.ToLower(strings.TrimPrefix(timer.GetStatus().String(), "TIMER_STATUS_")),
			"attempts": 0, "first": zone.cell(timer.GetCreatedAt()), "last": zone.cell(firstSet(timer.GetFiredAt(), timer.GetFireAt())),
			"duration": compactDuration(timer.GetDuration().AsDuration()), "detail": timerKind(timer.GetTimerKind()),
		}))
	}
	stored := storedPayloadTypes(description)
	for _, event := range description.GetEvents() {
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "event", "id": event.GetKey().GetEventId(), "status": "received",
			"attempts": 0, "first": zone.cell(event.GetReceivedAt()), "last": zone.cell(event.GetReceivedAt()),
			"duration": "", "detail": stored["event/"+event.GetKey().GetEventId()+".payload"],
		}))
	}
	for _, claim := range description.GetClaims() {
		table.Rows = append(table.Rows, row(map[string]any{
			"kind": "claim", "id": claim.GetKey().GetClaimId(), "status": "held",
			"attempts": 0, "first": zone.cell(claim.GetCreatedAt()), "last": zone.cell(claim.GetLeaseExpiresAt()),
			"duration": "", "detail": "owner " + claim.GetOwnerId(),
		}))
	}
	return table
}

// storedPayloadTypes maps each rendered payload's path to the short name of
// the type it was stored as, marked when the viewer's payloads are redacted.
// A record's own Any is not that type: DescribeRun replaces a payload this
// process cannot resolve, and every payload a redacted viewer sees, with an
// OpaquePayload stand-in. The RenderedPayload at the same path always names
// the stored type, even for a payload that really was stored as an
// OpaquePayload, which unwrapping the record's Any would misname.
func storedPayloadTypes(description *inspectionv1.DescribeRunResponse) map[string]string {
	types := make(map[string]string, len(description.GetPayloads()))
	for _, payload := range description.GetPayloads() {
		name := shortTypeURL(payload.GetTypeUrl())
		if payload.GetRedacted() {
			name += " · redacted"
		}
		types[payload.GetPath()] = name
	}
	return types
}

func payloadsObject(description *inspectionv1.DescribeRunResponse) *terminalv1.ObjectPayload {
	object := &terminalv1.ObjectPayload{
		ObjectType: "Payloads",
		ObjectId:   runID(description),
		Title:      "Inputs and results",
	}
	if description.GetPayloadVisibility() == inspectionv1.PayloadVisibility_PAYLOAD_VISIBILITY_REDACTED {
		object.Description = proto.String("Your access to this store shows payload types and sizes only.")
	}
	for _, payload := range description.GetPayloads() {
		path := payload.GetPath()
		group := "Workflow"
		if subject, _, found := strings.Cut(path, "."); found && subject != "workflow" {
			group = strings.Replace(subject, "/", " ", 1)
		}
		var value *structpb.Value
		switch {
		case payload.GetRedacted():
			value = structpb.NewStringValue(fmt.Sprintf("redacted · %d bytes", payload.GetSizeBytes()))
		case payload.GetJson() != nil:
			value = payload.GetJson()
		case payload.GetOpaque() != nil:
			encoded := base64.StdEncoding.EncodeToString(payload.GetOpaque())
			if len(encoded) > 96 {
				encoded = encoded[:96] + "…"
			}
			value = structpb.NewStringValue(fmt.Sprintf("no descriptor configured · %d bytes · %s", payload.GetSizeBytes(), encoded))
		default:
			value = structpb.NewStringValue(fmt.Sprintf("larger than the render limit · %d bytes", payload.GetSizeBytes()))
		}
		object.Properties = append(object.Properties, &terminalv1.ObjectProperty{
			Key:         path,
			Label:       path[strings.LastIndex(path, ".")+1:] + " · " + shortTypeURL(payload.GetTypeUrl()),
			Value:       value,
			Description: proto.String(fmt.Sprintf("%s · %d bytes", payload.GetTypeUrl(), payload.GetSizeBytes())),
			Group:       proto.String(group),
		})
	}
	return object
}

func prop(key, label, value, group string) *terminalv1.ObjectProperty {
	return &terminalv1.ObjectProperty{Key: key, Label: label, Value: structpb.NewStringValue(value), Group: proto.String(group)}
}

func timeProp(key, label string, value *timestamppb.Timestamp, group string, zone displayZone) *terminalv1.ObjectProperty {
	return prop(key, zone.label(label), defaultString(zone.cell(value), "—"), group)
}

func row(values map[string]any) *structpb.Struct {
	return structOf(values)
}

func structOf(values map[string]any) *structpb.Struct {
	fields := make(map[string]*structpb.Value, len(values))
	for key, value := range values {
		switch typed := value.(type) {
		case string:
			fields[key] = structpb.NewStringValue(typed)
		case int:
			fields[key] = structpb.NewNumberValue(float64(typed))
		default:
			fields[key] = structpb.NewNullValue()
		}
	}
	return &structpb.Struct{Fields: fields}
}

func optional(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// timestamp renders an instant for machine fields such as updated_at: RFC
// 3339 in UTC, whatever zone the viewer displays.
func timestamp(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().UTC().Format(time.RFC3339)
}

// displayZone renders instants for people in one time zone: UTC unless the
// viewer asks for another. Table cells leave the zone out because their
// column label names it; event timestamps and free text carry it.
type displayZone struct {
	location *time.Location
	name     string
}

var (
	utcZone         = displayZone{location: time.UTC, name: "UTC"}
	zoneNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+-]*(/[A-Za-z0-9_+-]+){0,2}$`)
)

// zone resolves the tz param: empty or UTC, or an IANA zone name. "Local"
// is refused because it would name the server's zone, not the viewer's.
func (service *TerminalService) zone(name string) (displayZone, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "UTC" {
		return utcZone, nil
	}
	if cached, ok := service.zones.Load(name); ok {
		return cached.(displayZone), nil
	}
	invalid := status.Error(codes.InvalidArgument, "tz must be UTC or an IANA time zone name such as Europe/Berlin")
	if name == "Local" || len(name) > 64 || !zoneNamePattern.MatchString(name) {
		return displayZone{}, invalid
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return displayZone{}, invalid
	}
	zone := displayZone{location: location, name: name}
	service.zones.Store(name, zone)
	return zone, nil
}

// label names the zone in a column or property label: "Started (UTC)".
func (zone displayZone) label(label string) string {
	return label + " (" + zone.name + ")"
}

// cell renders an instant for a column whose label names the zone.
func (zone displayZone) cell(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().In(zone.location).Format("2006-01-02 15:04:05")
}

// instant renders an ISO 8601 instant with its offset, for event timestamps.
func (zone displayZone) instant(value *timestamppb.Timestamp) string {
	if value == nil {
		return ""
	}
	return value.AsTime().In(zone.location).Format(time.RFC3339)
}

// clock renders a time of day with its zone abbreviation, for free text.
func (zone displayZone) clock(value *timestamppb.Timestamp) string {
	if value == nil {
		return "—"
	}
	return value.AsTime().In(zone.location).Format("15:04:05 MST")
}

func firstSet(values ...*timestamppb.Timestamp) *timestamppb.Timestamp {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

// relative renders an instant against now: "in 32s" or "44m late".
func relative(at, now time.Time) string {
	delta := at.Sub(now).Round(time.Second)
	if delta >= 0 {
		return "in " + compactDuration(delta)
	}
	return compactDuration(-delta) + " late"
}

func relativeOrEmpty(at *timestamppb.Timestamp, now time.Time) string {
	if at == nil {
		return ""
	}
	return relative(at.AsTime(), now)
}

func runDuration(start, end *timestamppb.Timestamp, now time.Time) string {
	if start == nil {
		return ""
	}
	if end == nil {
		return compactDuration(now.Sub(start.AsTime())) + " elapsed"
	}
	return compactDuration(end.AsTime().Sub(start.AsTime()))
}

func span(start, end *timestamppb.Timestamp) string {
	if start == nil || end == nil {
		return ""
	}
	return compactDuration(end.AsTime().Sub(start.AsTime()))
}

func compactDuration(value time.Duration) string {
	value = value.Round(time.Second)
	if value < 0 {
		value = 0
	}
	hours := int(value / time.Hour)
	minutes := int(value % time.Hour / time.Minute)
	seconds := int(value % time.Minute / time.Second)
	switch {
	case hours > 0:
		return fmt.Sprintf("%dh %02dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm %02ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

func failureText(failure *temporalessv1.ActivityFailure) string {
	if failure == nil {
		return ""
	}
	switch {
	case failure.GetCode() != "" && failure.GetMessage() != "":
		return failure.GetCode() + ": " + failure.GetMessage()
	case failure.GetCode() != "":
		return failure.GetCode()
	default:
		return failure.GetMessage()
	}
}

func timerKind(kind temporalessv1.TimerKind) string {
	switch kind {
	case temporalessv1.TimerKind_TIMER_KIND_SLEEP:
		return "sleep"
	case temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY:
		return "activity retry"
	case temporalessv1.TimerKind_TIMER_KIND_POLL:
		return "poll"
	default:
		return "timer"
	}
}

func ledgerText(state inspectionv1.WakeLedgerState) string {
	switch state {
	case inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_AGREES:
		return "agrees"
	case inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_MISSING:
		return "timer record missing; the scanner repairs it"
	case inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_DIFFERS:
		return "timer record differs; the scanner repairs it"
	case inspectionv1.WakeLedgerState_WAKE_LEDGER_STATE_CANONICAL_UNREADABLE:
		return "timer record unreadable"
	default:
		return ""
	}
}

// shortType renders "workflow:pkg.Req->pkg.Resp" as "Req → Resp".
func shortType(workflowType string) string {
	if workflowType == "" {
		return ""
	}
	body := workflowType
	if _, rest, found := strings.Cut(workflowType, ":"); found {
		body = rest
	}
	request, response, found := strings.Cut(body, "->")
	if !found {
		return workflowType
	}
	return lastSegment(request) + " → " + lastSegment(response)
}

func shortTypeURL(typeURL string) string {
	return lastSegment(typeURL[strings.LastIndex(typeURL, "/")+1:])
}

func lastSegment(name string) string {
	return name[strings.LastIndex(name, ".")+1:]
}
