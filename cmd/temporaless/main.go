// temporaless is a thin operator CLI over the existing inspector / janitor /
// store adapters. It exists as a transitional surface — once the protobuf
// service is migrated to invariantprotocol, the CLI (and MCP) will be
// generated automatically from the proto, and this binary will be retired.
//
// To keep the eventual swap painless, every subcommand maps 1:1 to a single
// adapter function. No business logic lives here.
//
// This transitional binary registers only OpenDAL `fs` via --store-scheme /
// --store-root. Cloud deployments should use authenticated remote
// RecordStoreService / RecordQueryService tooling instead of adding cloud
// credentials and drivers to this local operator binary. Output is text by
// default; --json switches to protojson for machine consumption.
//
// Subcommands:
//
//	list-workflows   --status STATUS [--workflow-id ID]
//	list-activities  --workflow-id ID --run-id RID
//	get-workflow     --workflow-id ID --run-id RID
//	reset-workflow   --workflow-id ID --run-id RID
//	reset-activity   --workflow-id ID --run-id RID --activity-id AID
//	reset-event      --workflow-id ID --run-id RID --event-id EID
//	sweep            --max-age DURATION
//	stale-workflows  --older-than DURATION
//	tail             [--poll-interval DURATION] [--status STATUS]
//	export           --kind {workflow,activity,timer,event} [--output FILE]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/inspector"
	"github.com/jim-technologies/temporaless/adapters/go/janitor"
	"github.com/jim-technologies/temporaless/adapters/go/scanquery"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// schemeRegistry maps the user-facing --store-scheme flag to OpenDAL Schemes.
// Only `fs` is wired into this transitional local CLI. Production cloud
// operators should use authenticated remote tooling over RecordStoreService /
// RecordQueryService rather than embedding cloud drivers and credentials here.
var schemeRegistry = map[string]opendal.Scheme{
	"fs": fs.Scheme,
}

const usage = `temporaless — operator CLI for the storage-first workflow framework.

USAGE
  temporaless [global flags] <subcommand> [flags]

GLOBAL FLAGS
  --store-scheme   OpenDAL scheme (only "fs" is registered).
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
  stale-workflows  List IN_PROGRESS workflows older than --older-than.
  tail             Stream new workflow records as they are written (poll loop).
  export           Bulk-decode records under a prefix to JSON Lines (audit / analytics).

Run "temporaless <subcommand> --help" for subcommand-specific flags.

This is a transitional local-filesystem CLI. For cloud stores, use
authenticated ConnectStore/RecordQueryService clients or generated remote
operator tooling; this binary intentionally does not bundle cloud drivers.
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

	store, query, cleanup, err := openStore(global.scheme, global.root)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer cleanup()

	switch subcommand {
	case "list-workflows":
		return cmdListWorkflows(ctx, query, global, subArgs, stdout)
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
		return cmdSweep(ctx, query, store, subArgs, stdout)
	case "stale-workflows":
		return cmdStaleWorkflows(ctx, query, global, subArgs, stdout)
	case "tail":
		return cmdTail(ctx, query, global, subArgs, stdout)
	case "export":
		return cmdExport(ctx, store, query, subArgs, stdout)
	default:
		fmt.Fprint(stderr, usage)
		return fmt.Errorf("unknown subcommand %q", subcommand)
	}
}

func openStore(scheme, root string) (storage.Store, storage.QueryStore, func(), error) {
	resolved, ok := schemeRegistry[scheme]
	if !ok {
		supported := make([]string, 0, len(schemeRegistry))
		for k := range schemeRegistry {
			supported = append(supported, k)
		}
		return nil, nil, func() {}, fmt.Errorf("unsupported --store-scheme %q (supported: %s)", scheme, strings.Join(supported, ", "))
	}
	operator, err := opendal.NewOperator(resolved, opendal.OperatorOptions{
		"root": root,
	})
	if err != nil {
		return nil, nil, func() {}, err
	}
	point := storage.NewOpenDALStore(operator)
	query, err := scanquery.New(operator, point, nil)
	if err != nil {
		operator.Close()
		return nil, nil, func() {}, err
	}
	return point, query, operator.Close, nil
}

// list-workflows ------------------------------------------------------------

func cmdListWorkflows(ctx context.Context, query storage.WorkflowQueryStore, g globalOpts, args []string, stdout io.Writer) error {
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
	response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
		Namespace:  namespaceFlag,
		WorkflowId: workflowIDFlag,
		Status:     status,
	})
	if err != nil {
		return err
	}
	records := response.GetRecords()
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

func cmdSweep(ctx context.Context, query storage.WorkflowQueryStore, store storage.Store, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("sweep", flag.ContinueOnError)
	var maxAge time.Duration
	fs.DurationVar(&maxAge, "max-age", 0, "Delete COMPLETED runs older than this (required, e.g. 168h for 7 days)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if maxAge <= 0 {
		return errors.New("--max-age must be > 0")
	}
	deleted, err := janitor.Sweep(ctx, query, store, nil, &temporalessv1.SweepRequest{
		Now:    timestamppb.New(time.Now().UTC()),
		MaxAge: durationpb.New(maxAge),
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "deleted %d runs\n", deleted)
	return nil
}

// stale-workflows ----------------------------------------------------------

// cmdStaleWorkflows lists IN_PROGRESS workflows whose `created_at` is older
// than the operator-supplied threshold. Wire this into alerting: a workflow
// stuck IN_PROGRESS for hours past its expected duration usually means a
// stuck timer-scanner, a missing event delivery, or an exhausted claim leak.
func cmdStaleWorkflows(ctx context.Context, query storage.WorkflowQueryStore, g globalOpts, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("stale-workflows", flag.ContinueOnError)
	var olderThan time.Duration
	var namespace string
	fs.DurationVar(&olderThan, "older-than", 0, "Threshold for considering an IN_PROGRESS workflow stale (required, e.g. 1h)")
	fs.StringVar(&namespace, "namespace", "", "Filter by namespace (default: all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if olderThan <= 0 {
		return errors.New("--older-than must be > 0")
	}
	inFlight, err := inspector.ListInFlightWorkflows(ctx, query)
	if err != nil {
		return err
	}
	cutoff := time.Now().UTC().Add(-olderThan)
	var stale []*temporalessv1.WorkflowRecord
	for _, r := range inFlight {
		if namespace != "" && r.GetKey().GetNamespace() != namespace {
			continue
		}
		if r.GetCreatedAt() == nil {
			continue
		}
		if r.GetCreatedAt().AsTime().After(cutoff) {
			continue
		}
		stale = append(stale, r)
	}
	if g.json {
		return emitJSONList(stdout, stale)
	}
	now := time.Now().UTC()
	for _, r := range stale {
		key := storage.WorkflowKeyFromProto(r.GetKey())
		age := now.Sub(r.GetCreatedAt().AsTime()).Truncate(time.Second)
		fmt.Fprintf(stdout, "%s\t%s/%s\t%s\n", age, key.WorkflowID, key.RunID, r.GetWorkflowType())
	}
	return nil
}

// tail ---------------------------------------------------------------------

// cmdTail polls the store for new workflow records and emits one line per
// new (workflow_id, run_id) seen. Useful for babysitting a backfill or a
// freshly-deployed pipeline. Status filter narrows the stream.
//
// The poll interval defaults to 2s. Caller exits the loop with Ctrl-C; ctx
// cancellation also unwinds cleanly.
func cmdTail(ctx context.Context, query storage.WorkflowQueryStore, g globalOpts, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("tail", flag.ContinueOnError)
	var pollInterval time.Duration
	var statusFlag, namespace, workflowID string
	fs.DurationVar(&pollInterval, "poll-interval", 2*time.Second, "How often to poll the store")
	fs.StringVar(&statusFlag, "status", "", "Filter: in-progress | completed | failed (default: all)")
	fs.StringVar(&namespace, "namespace", "", "Filter by namespace (default: all)")
	fs.StringVar(&workflowID, "workflow-id", "", "Filter by workflow_id (default: all)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if pollInterval <= 0 {
		return errors.New("--poll-interval must be > 0")
	}
	status, err := parseWorkflowStatus(statusFlag)
	if err != nil {
		return err
	}

	seen := make(map[string]temporalessv1.WorkflowStatus)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	emit := func() error {
		response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
			Status:     status,
		})
		if err != nil {
			return err
		}
		records := response.GetRecords()
		for _, r := range records {
			key := storage.WorkflowKeyFromProto(r.GetKey())
			composite := fmt.Sprintf("%s|%s|%s", key.Namespace, key.WorkflowID, key.RunID)
			prevStatus, ok := seen[composite]
			if ok && prevStatus == r.GetStatus() {
				continue
			}
			seen[composite] = r.GetStatus()
			if g.json {
				if err := emitJSON(stdout, r); err != nil {
					return err
				}
				continue
			}
			ts := time.Now().UTC().Format(time.RFC3339)
			fmt.Fprintf(stdout, "%s\t%s\t%s/%s\t%s\n",
				ts, r.GetStatus().String(),
				key.WorkflowID, key.RunID,
				r.GetWorkflowType(),
			)
		}
		return nil
	}

	if err := emit(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

// export -------------------------------------------------------------------

// cmdExport bulk-decodes records under a prefix and emits one decoded
// protojson object per line. Useful for ingesting audit data into BigQuery /
// DuckDB / dbt / Snowflake — operators don't need our decoder or runtime;
// they just need `aws s3 cp` access to the bucket and `temporaless export`
// to read the binpb.
//
// Output: stdout by default; --output FILE for redirection. One JSONL record
// per stored record; each line is independently parseable.
func cmdExport(
	ctx context.Context,
	store storage.Store,
	query storage.WorkflowQueryStore,
	args []string,
	stdout io.Writer,
) (returnErr error) {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	var kind, namespace, workflowID, runID, output string
	fs.StringVar(&kind, "kind", "", "Record kind: workflow | activity | timer | event (required)")
	fs.StringVar(&namespace, "namespace", "", "Filter by namespace (workflow only; default: all)")
	fs.StringVar(&workflowID, "workflow-id", "", "Filter by workflow_id (required for activity/timer/event)")
	fs.StringVar(&runID, "run-id", "", "Run ID (required for activity/timer/event)")
	fs.StringVar(&output, "output", "", "Write JSON Lines to this file (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if kind == "" {
		return errors.New("--kind is required (workflow | activity | timer | event)")
	}

	exported := 0
	defer func() {
		if returnErr == nil {
			fmt.Fprintf(os.Stderr, "exported %d %s records\n", exported, kind)
		}
	}()

	w := stdout
	if output != "" {
		secure, err := openSecureExportFile(output)
		if err != nil {
			return err
		}
		defer func() {
			returnErr = secure.finish(returnErr)
		}()
		w = secure.file
	}

	emit := func(message proto.Message) error {
		data, err := protojson.Marshal(message)
		if err != nil {
			return err
		}
		_, err = w.Write(append(data, '\n'))
		return err
	}

	switch kind {
	case "workflow":
		response, err := query.ListWorkflows(ctx, &temporalessv1.ListWorkflowsRequest{
			Namespace:  namespace,
			WorkflowId: workflowID,
		})
		if err != nil {
			return err
		}
		records := response.GetRecords()
		for _, r := range records {
			if err := emit(r); err != nil {
				return err
			}
		}
		exported = len(records)
		return nil
	case "activity":
		if workflowID == "" || runID == "" {
			return errors.New("--workflow-id and --run-id are required when --kind=activity")
		}
		key := workflowKeyFor(namespace, workflowID, runID)
		records, err := store.ListActivities(ctx, key)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := emit(r); err != nil {
				return err
			}
		}
		exported = len(records)
		return nil
	case "timer":
		if workflowID == "" || runID == "" {
			return errors.New("--workflow-id and --run-id are required when --kind=timer")
		}
		key := workflowKeyFor(namespace, workflowID, runID)
		records, err := store.ListTimers(ctx, key, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := emit(r); err != nil {
				return err
			}
		}
		exported = len(records)
		return nil
	case "event":
		if workflowID == "" || runID == "" {
			return errors.New("--workflow-id and --run-id are required when --kind=event")
		}
		key := workflowKeyFor(namespace, workflowID, runID)
		records, err := store.ListEvents(ctx, key)
		if err != nil {
			return err
		}
		for _, r := range records {
			if err := emit(r); err != nil {
				return err
			}
		}
		exported = len(records)
		return nil
	default:
		return fmt.Errorf("unknown --kind %q (want: workflow | activity | timer | event)", kind)
	}
}

type secureExportFile struct {
	file      *os.File
	finalPath string
	tempPath  string
}

func openSecureExportFile(path string) (*secureExportFile, error) {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return nil, err
	}
	cleanup := func(operationErr error) (*secureExportFile, error) {
		if closeErr := f.Close(); closeErr != nil {
			operationErr = errors.Join(operationErr, closeErr)
		}
		if removeErr := os.Remove(f.Name()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			operationErr = errors.Join(operationErr, removeErr)
		}
		return nil, operationErr
	}
	if err := f.Chmod(0o600); err != nil {
		return cleanup(err)
	}
	return &secureExportFile{
		file:      f,
		finalPath: path,
		tempPath:  f.Name(),
	}, nil
}

func (export *secureExportFile) finish(operationErr error) error {
	if operationErr == nil {
		operationErr = export.file.Sync()
	}
	if closeErr := export.file.Close(); closeErr != nil {
		operationErr = errors.Join(operationErr, closeErr)
	}
	if operationErr == nil {
		operationErr = os.Rename(export.tempPath, export.finalPath)
	}
	if operationErr != nil {
		if removeErr := os.Remove(export.tempPath); removeErr != nil &&
			!errors.Is(removeErr, os.ErrNotExist) {
			operationErr = errors.Join(operationErr, removeErr)
		}
	}
	return operationErr
}

func workflowKeyFor(namespace, workflowID, runID string) storage.WorkflowKey {
	if namespace == "" {
		namespace = storage.DefaultNamespace
	}
	return storage.WorkflowKey{
		Namespace:  namespace,
		WorkflowID: workflowID,
		RunID:      runID,
	}
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
