package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/visualization"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	runDescriptionFormatVersion uint32 = 1
	runSnapshotConsistency             = "non-atomic"
)

// runDescriptionJSON is a CLI view, not a storage or RPC contract. Its record
// values retain protobuf JSON field names; application Any values use the
// descriptor-free representation produced by marshalCLIProto.
type runDescriptionJSON struct {
	FormatVersion       uint32            `json:"formatVersion"`
	SnapshotConsistency string            `json:"snapshotConsistency"`
	Key                 json.RawMessage   `json:"key"`
	Workflow            json.RawMessage   `json:"workflow"`
	Activities          []json.RawMessage `json:"activities"`
	Timers              []json.RawMessage `json:"timers"`
	Events              []json.RawMessage `json:"events"`
	Claims              []json.RawMessage `json:"claims"`
	ClaimsInspected     bool              `json:"claimsInspected"`
}

// cmdDescribeRun composes existing authoritative point reads. It deliberately
// does not require a cross-run query index and does not claim that its several
// reads form a transactionally consistent snapshot.
func cmdDescribeRun(
	ctx context.Context,
	store storage.Store,
	claimLister visualization.ClaimLister,
	g globalOpts,
	args []string,
	stdout io.Writer,
) error {
	fs := flag.NewFlagSet("describe-run", flag.ContinueOnError)
	var workflowID, runID, namespace string
	fs.StringVar(&workflowID, "workflow-id", "", "Workflow ID (required)")
	fs.StringVar(&runID, "run-id", "", "Run ID (required)")
	fs.StringVar(&namespace, "namespace", storage.DefaultNamespace, "Namespace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if workflowID == "" || runID == "" {
		return errors.New("--workflow-id and --run-id are required")
	}

	key := storage.WorkflowKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
	inspection, err := visualization.InspectRun(ctx, store, claimLister, key)
	if err != nil {
		return err
	}
	if runInspectionEmpty(inspection) {
		return fmt.Errorf("run %s/%s has no inspectable records", workflowID, runID)
	}

	var output []byte
	if g.json {
		output, err = marshalRunDescription(inspection)
	} else {
		output = []byte(formatRunDescription(inspection))
	}
	if err != nil {
		return err
	}
	_, err = stdout.Write(output)
	return err
}

func runInspectionEmpty(inspection *visualization.RunInspection) bool {
	return inspection.Workflow == nil &&
		len(inspection.Activities) == 0 &&
		len(inspection.Timers) == 0 &&
		len(inspection.Events) == 0 &&
		len(inspection.Claims) == 0
}

func marshalRunDescription(inspection *visualization.RunInspection) ([]byte, error) {
	key, err := marshalCLIProto(inspection.Key.Proto())
	if err != nil {
		return nil, fmt.Errorf("marshal workflow key: %w", err)
	}
	view := runDescriptionJSON{
		FormatVersion:       runDescriptionFormatVersion,
		SnapshotConsistency: runSnapshotConsistency,
		Key:                 key,
		Activities:          make([]json.RawMessage, 0, len(inspection.Activities)),
		Timers:              make([]json.RawMessage, 0, len(inspection.Timers)),
		Events:              make([]json.RawMessage, 0, len(inspection.Events)),
		Claims:              make([]json.RawMessage, 0, len(inspection.Claims)),
		ClaimsInspected:     inspection.ClaimsInspected,
	}
	if inspection.Workflow != nil {
		view.Workflow, err = marshalCLIProto(inspection.Workflow)
		if err != nil {
			return nil, fmt.Errorf("marshal workflow record: %w", err)
		}
	}
	if err := appendCLIRecords(&view.Activities, inspection.Activities); err != nil {
		return nil, fmt.Errorf("marshal activity records: %w", err)
	}
	if err := appendCLIRecords(&view.Timers, inspection.Timers); err != nil {
		return nil, fmt.Errorf("marshal timer records: %w", err)
	}
	if err := appendCLIRecords(&view.Events, inspection.Events); err != nil {
		return nil, fmt.Errorf("marshal event records: %w", err)
	}
	if err := appendCLIRecords(&view.Claims, inspection.Claims); err != nil {
		return nil, fmt.Errorf("marshal claim records: %w", err)
	}
	data, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func appendCLIRecords[T proto.Message](destination *[]json.RawMessage, records []T) error {
	for _, record := range records {
		data, err := marshalCLIProto(record)
		if err != nil {
			return err
		}
		*destination = append(*destination, data)
	}
	return nil
}

func formatRunDescription(inspection *visualization.RunInspection) string {
	key := inspection.Key
	output := fmt.Sprintf(
		"run=%s/%s/%s\nsnapshot_consistency=%s\n",
		key.Namespace,
		key.WorkflowID,
		key.RunID,
		runSnapshotConsistency,
	)
	if inspection.Workflow == nil {
		output += "workflow=missing\n"
	} else {
		workflow := inspection.Workflow
		output += fmt.Sprintf(
			"workflow\tstatus=%s\ttype=%s\tcreated_at=%s\tcompleted_at=%s%s%s%s\n",
			workflow.GetStatus().String(),
			workflow.GetWorkflowType(),
			formatTimestamp(workflow.GetCreatedAt()),
			formatTimestamp(workflow.GetCompletedAt()),
			formatFailure(workflow.GetFailure()),
			formatNamedAny("input", workflow.GetInput()),
			formatNamedAny("result", workflow.GetResult()),
		)
		output += formatAnnotations("workflow_annotation", workflow.GetAnnotations())
	}

	for _, activity := range inspection.Activities {
		activityID := activity.GetKey().GetActivityId()
		output += fmt.Sprintf(
			"activity\t%s\tstatus=%s\ttype=%s\tattempts=%d\tcreated_at=%s\tcompleted_at=%s\tnext_attempt_at=%s%s%s%s\n",
			activityID,
			activity.GetStatus().String(),
			activity.GetActivityType(),
			len(activity.GetAttempts()),
			formatTimestamp(activity.GetCreatedAt()),
			formatTimestamp(activity.GetCompletedAt()),
			formatTimestamp(activity.GetNextAttemptAt()),
			formatFailure(activity.GetFailure()),
			formatNamedAny("input", activity.GetInput()),
			formatNamedAny("result", activity.GetResult()),
		)
		for _, attempt := range activity.GetAttempts() {
			output += fmt.Sprintf(
				"attempt\t%s\t%d\tstarted_at=%s\tcompleted_at=%s%s\n",
				activityID,
				attempt.GetAttempt(),
				formatTimestamp(attempt.GetStartedAt()),
				formatTimestamp(attempt.GetCompletedAt()),
				formatFailure(attempt.GetFailure()),
			)
		}
		output += formatAnnotations("activity_annotation\t"+activityID, activity.GetAnnotations())
	}

	for _, timer := range inspection.Timers {
		output += fmt.Sprintf(
			"timer\t%s\tstatus=%s\tkind=%s\tduration=%s\tfire_at=%s\tfired_at=%s\tretry_activity_id=%s\n",
			timer.GetKey().GetTimerId(),
			timer.GetStatus().String(),
			timer.GetTimerKind().String(),
			timer.GetDuration().AsDuration(),
			formatTimestamp(timer.GetFireAt()),
			formatTimestamp(timer.GetFiredAt()),
			timer.GetRetryActivityId(),
		)
	}

	for _, event := range inspection.Events {
		output += fmt.Sprintf(
			"event\t%s\treceived_at=%s%s\n",
			event.GetKey().GetEventId(),
			formatTimestamp(event.GetReceivedAt()),
			formatNamedAny("payload", event.GetPayload()),
		)
	}

	if !inspection.ClaimsInspected {
		return output + "claims=not-inspected\n"
	}
	for _, claim := range inspection.Claims {
		output += fmt.Sprintf(
			"claim\t%s\tresource_type=%s\tresource_id=%s\towner_id=%s\tlease_expires_at=%s\theartbeat_at=%s\n",
			claim.GetKey().GetClaimId(),
			claim.GetResourceType().String(),
			claim.GetResourceId(),
			claim.GetOwnerId(),
			formatTimestamp(claim.GetLeaseExpiresAt()),
			formatTimestamp(claim.GetHeartbeatAt()),
		)
	}
	return output + fmt.Sprintf("claims_inspected=true\tcount=%d\n", len(inspection.Claims))
}

func formatTimestamp(timestamp *timestamppb.Timestamp) string {
	if timestamp == nil {
		return "-"
	}
	return timestamp.AsTime().Format(time.RFC3339Nano)
}

func formatFailure(failure *temporalessv1.ActivityFailure) string {
	if failure == nil {
		return ""
	}
	return fmt.Sprintf("\tfailure_code=%q\tfailure_message=%q", failure.GetCode(), failure.GetMessage())
}

func formatNamedAny(name string, payload *anypb.Any) string {
	if payload == nil {
		return ""
	}
	return fmt.Sprintf(
		"\t%s_type=%q\t%s_bytes=%d",
		name,
		payload.GetTypeUrl(),
		name,
		len(payload.GetValue()),
	)
}

func formatAnnotations(prefix string, annotations map[string]string) string {
	keys := make([]string, 0, len(annotations))
	for key := range annotations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	output := ""
	for _, key := range keys {
		output += fmt.Sprintf("%s\t%s=%q\n", prefix, key, annotations[key])
	}
	return output
}
