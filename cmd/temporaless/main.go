// temporaless is a thin operator CLI over the existing inspector / janitor /
// store adapters. It exists as a transitional surface — once the protobuf
// service is migrated to invariantprotocol, the CLI (and MCP) will be
// generated automatically from the proto, and this binary will be retired.
//
// To keep the eventual swap painless, every subcommand maps 1:1 to a single
// adapter function. No business logic lives here.
//
// Storage backend is selected via --store-scheme / --store-root (OpenDAL
// schemes such as `fs`, `s3`, `gcs`). Output is text by default; --json
// switches to protojson for machine consumption.
//
// Subcommands:
//
//	list-workflows --status STATUS [--workflow-id ID]
//	list-activities --workflow-id ID --run-id RID
//	get-workflow    --workflow-id ID --run-id RID
//	reset-workflow  --workflow-id ID --run-id RID
//	reset-activity  --workflow-id ID --run-id RID --activity-id AID
//	reset-event     --workflow-id ID --run-id RID --event-id EID
//	sweep           --max-age DURATION
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/inspector"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// schemeRegistry maps the user-facing --store-scheme flag to OpenDAL Schemes.
// Only `fs` is wired by default. To support s3/gcs/azblob, import the relevant
// opendal-go-services package and add an entry here. The CLI is a transitional
// surface; once invariantprotocol generates the CLI from proto + a generic
// store factory, this lookup goes away.
var schemeRegistry = map[string]opendal.Scheme{
	"fs": fs.Scheme,
}

const usage = `temporaless — operator CLI for the storage-first workflow framework.

USAGE
  temporaless [global flags] <subcommand> [flags]

GLOBAL FLAGS
  --store-scheme   OpenDAL scheme (default: "fs"). Examples: fs, s3, gcs.
  --store-root     OpenDAL root path/bucket (required).
  --json           Output records as protojson instead of text summaries.

SUBCOMMANDS
  list-workflows   List workflow records, optionally filtered by status.
  list-activities  List activity records under a (workflow_id, run_id).
  get-workflow     Read and print a workflow record.
  reset-workflow   Delete a workflow record so the next invocation re-executes.
  reset-activity   Delete an activity record so its body re-executes.
  reset-event      Delete a stored event record so wait_event re-raises pending.
  sweep            Delete COMPLETED runs older than --max-age.

Run "temporaless <subcommand> --help" for subcommand-specific flags.
`

type globalOpts struct {
	scheme string
	root   string
	json   bool
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return errors.New("subcommand required")
	}

	// Global flags precede the subcommand. flag.Parse stops at the first
	// non-flag argument, which is the subcommand name.
	global := globalOpts{scheme: "fs"}
	gfs := flag.NewFlagSet("global", flag.ContinueOnError)
	gfs.SetOutput(io.Discard) // surface our own usage instead of flag's
	gfs.StringVar(&global.scheme, "store-scheme", "fs", "OpenDAL scheme")
	gfs.StringVar(&global.root, "store-root", "", "OpenDAL root path/bucket")
	gfs.BoolVar(&global.json, "json", false, "Output protojson instead of text summaries")
	if err := gfs.Parse(args); err != nil {
		return err
	}
	remaining := gfs.Args()
	if len(remaining) == 0 {
		fmt.Fprint(stderr, usage)
		return errors.New("subcommand required")
	}
	subcommand := remaining[0]
	subArgs := remaining[1:]
	if subcommand == "help" || subcommand == "-h" || subcommand == "--help" {
		fmt.Fprint(stdout, usage)
		return nil
	}
	if global.root == "" {
		return errors.New("--store-root is required")
	}

	store, cleanup, err := openStore(global.scheme, global.root)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer cleanup()

	switch subcommand {
	case "list-workflows":
		return cmdListWorkflows(ctx, store, global, subArgs, stdout)
	case "list-activities":
		return cmdListActivities(ctx, store, global, subArgs, stdout)
	case "get-workflow":
		return cmdGetWorkflow(ctx, store, global, subArgs, stdout)
	case "reset-workflow":
		return cmdResetWorkflow(ctx, store, subArgs, stdout)
	case "reset-activity":
		return cmdResetActivity(ctx, store, subArgs, stdout)
	case "reset-event":
		return cmdResetEvent(ctx, store, subArgs, stdout)
	case "sweep":
		return cmdSweep(ctx, store, subArgs, stdout)
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}
}

func openStore(scheme, root string) (storage.Store, func(), error) {
	resolved, ok := schemeRegistry[scheme]
	if !ok {
		supported := make([]string, 0, len(schemeRegistry))
		for k := range schemeRegistry {
			supported = append(supported, k)
		}
		return nil, func() {}, fmt.Errorf("unsupported --store-scheme %q (supported: %s)", scheme, strings.Join(supported, ", "))
	}
	operator, err := opendal.NewOperator(resolved, opendal.OperatorOptions{
		"root": root,
	})
	if err != nil {
		return nil, func() {}, err
	}
	return storage.NewOpenDALStore(operator), operator.Close, nil
}

// list-workflows ------------------------------------------------------------

func cmdListWorkflows(ctx context.Context, store storage.Store, g globalOpts, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("list-workflows", flag.ContinueOnError)
	var statusFlag, namespaceFlag, workflowIDFlag string
	fs.StringVar(&statusFlag, "status", "", "Filter: in-progress | completed | failed (default: all)")
	fs.StringVar(&namespaceFlag, "namespace", "", "Filter by namespace (default: all)")
	fs.StringVar(&workflowIDFlag, "workflow-id", "", "Filter by workflow_id (default: all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	status, err := parseWorkflowStatus(statusFlag)
	if err != nil {
		return err
	}
	records, err := store.ListWorkflows(ctx, namespaceFlag, workflowIDFlag, status)
	if err != nil {
		return err
	}
	if g.json {
		return emitJSONList(stdout, records)
	}
	for _, r := range records {
		key := storage.WorkflowKeyFromProto(r.GetKey())
		fmt.Fprintf(stdout, "%s\t%s/%s\t%s\n",
			r.GetStatus().String(),
			key.WorkflowID, key.RunID,
			r.GetWorkflowType(),
		)
	}
	return nil
}

// list-activities ----------------------------------------------------------

func cmdListActivities(ctx context.Context, store storage.Store, g globalOpts, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("list-activities", flag.ContinueOnError)
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
	key := storage.WorkflowKey{Namespace: namespace, WorkflowID: workflowID, RunID: runID}
	records, err := inspector.ListActivities(ctx, store, key)
	if err != nil {
		return err
	}
	if g.json {
		return emitJSONList(stdout, records)
	}
	for _, r := range records {
		actKey := storage.ActivityKeyFromProto(r.GetKey())
		fmt.Fprintf(stdout, "%s\t%s\t%s\tattempts=%d\n",
			r.GetStatus().String(),
			actKey.ActivityID,
			r.GetActivityType(),
			len(r.GetAttempts()),
		)
	}
	return nil
}

// get-workflow -------------------------------------------------------------

func cmdGetWorkflow(ctx context.Context, store storage.Store, g globalOpts, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("get-workflow", flag.ContinueOnError)
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
	key := storage.WorkflowKey{Namespace: namespace, WorkflowID: workflowID, RunID: runID}
	record, found, err := store.GetWorkflow(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("workflow %s/%s not found", workflowID, runID)
	}
	if g.json {
		return emitJSON(stdout, record)
	}
	fmt.Fprintf(stdout, "status=%s\n", record.GetStatus().String())
	fmt.Fprintf(stdout, "workflow_type=%s\n", record.GetWorkflowType())
	fmt.Fprintf(stdout, "code_version=%s\n", record.GetCodeVersion())
	fmt.Fprintf(stdout, "input_digest=%s\n", record.GetInputDigest())
	if record.GetCreatedAt() != nil {
		fmt.Fprintf(stdout, "created_at=%s\n", record.GetCreatedAt().AsTime().Format(time.RFC3339Nano))
	}
	if record.GetCompletedAt() != nil {
		fmt.Fprintf(stdout, "completed_at=%s\n", record.GetCompletedAt().AsTime().Format(time.RFC3339Nano))
	}
	for k, v := range record.GetAnnotations() {
		fmt.Fprintf(stdout, "annotation\t%s=%s\n", k, v)
	}
	return nil
}

// reset-workflow / reset-activity / reset-event ----------------------------

func cmdResetWorkflow(ctx context.Context, store storage.Store, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("reset-workflow", flag.ContinueOnError)
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
	key := storage.WorkflowKey{Namespace: namespace, WorkflowID: workflowID, RunID: runID}
	if err := inspector.ResetWorkflow(ctx, store, key); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "reset workflow %s/%s\n", workflowID, runID)
	return nil
}

func cmdResetActivity(ctx context.Context, store storage.Store, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("reset-activity", flag.ContinueOnError)
	var workflowID, runID, activityID, namespace string
	fs.StringVar(&workflowID, "workflow-id", "", "Workflow ID (required)")
	fs.StringVar(&runID, "run-id", "", "Run ID (required)")
	fs.StringVar(&activityID, "activity-id", "", "Activity ID (required)")
	fs.StringVar(&namespace, "namespace", storage.DefaultNamespace, "Namespace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if workflowID == "" || runID == "" || activityID == "" {
		return errors.New("--workflow-id, --run-id, and --activity-id are required")
	}
	key := storage.ActivityKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
		ActivityID: activityID,
	}
	if err := inspector.ResetActivity(ctx, store, key); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "reset activity %s/%s/%s\n", workflowID, runID, activityID)
	return nil
}

func cmdResetEvent(ctx context.Context, store storage.Store, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("reset-event", flag.ContinueOnError)
	var workflowID, runID, eventID, namespace string
	fs.StringVar(&workflowID, "workflow-id", "", "Workflow ID (required)")
	fs.StringVar(&runID, "run-id", "", "Run ID (required)")
	fs.StringVar(&eventID, "event-id", "", "Event ID (required)")
	fs.StringVar(&namespace, "namespace", storage.DefaultNamespace, "Namespace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if workflowID == "" || runID == "" || eventID == "" {
		return errors.New("--workflow-id, --run-id, and --event-id are required")
	}
	key := storage.EventKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
		EventID:    eventID,
	}
	if err := inspector.ResetEvent(ctx, store, key); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "reset event %s/%s/%s\n", workflowID, runID, eventID)
	return nil
}

// sweep --------------------------------------------------------------------

func cmdSweep(ctx context.Context, store storage.Store, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sweep", flag.ContinueOnError)
	var maxAge time.Duration
	fs.DurationVar(&maxAge, "max-age", 0, "Delete COMPLETED runs older than this (required, e.g. 168h for 7 days)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if maxAge <= 0 {
		return errors.New("--max-age must be > 0")
	}
	deleted, err := janitor.Sweep(ctx, store, time.Now().UTC(), maxAge)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "deleted %d runs\n", deleted)
	return nil
}

// helpers ------------------------------------------------------------------

func parseWorkflowStatus(s string) (temporalessv1.WorkflowStatus, error) {
	switch strings.ToLower(s) {
	case "", "all":
		return temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, nil
	case "in-progress", "in_progress":
		return temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, nil
	case "completed":
		return temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, nil
	case "failed":
		return temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED, nil
	default:
		return temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, fmt.Errorf("unknown status %q (want: in-progress|completed|failed|all)", s)
	}
}

func emitJSON(w io.Writer, message proto.Message) error {
	data, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(message)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}

func emitJSONList[T proto.Message](w io.Writer, records []T) error {
	// emit as a JSON array of protojson messages
	items := make([]json.RawMessage, 0, len(records))
	for _, r := range records {
		b, err := protojson.Marshal(r)
		if err != nil {
			return err
		}
		items = append(items, b)
	}
	data, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}
