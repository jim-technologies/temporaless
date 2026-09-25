package inspection_test

import (
	"context"
	"crypto/sha256"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	opendalfs "github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// now is the fixed clock every test's Service reads.
var now = time.Date(2026, 9, 25, 8, 14, 0, 0, time.UTC)

func at(offset time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(now.Add(offset))
}

type fixture struct {
	t        *testing.T
	root     string
	operator *opendal.Operator
	records  *storage.OpenDALStore
	store    *inspection.Store
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	operator, err := opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	records := storage.NewOpenDALStore(operator)
	return &fixture{
		t:        t,
		root:     root,
		operator: operator,
		records:  records,
		store: &inspection.Store{
			ID:          "engine",
			DisplayName: "Data engine",
			Namespaces:  []string{"default", "backfill"},
			ListClaims:  true,
			Records:     records,
			Bucket:      inspection.NewOpenDALBucket(operator),
		},
	}
}

func (f *fixture) service(options inspection.Options) *inspection.Service {
	f.t.Helper()
	if options.Access == nil {
		options.Access = inspection.AllowAll
	}
	if options.Now == nil {
		options.Now = func() time.Time { return now }
	}
	service, err := inspection.NewService([]*inspection.Store{f.store}, options)
	if err != nil {
		f.t.Fatal(err)
	}
	return service
}

func key(workflowID, runID string) storage.WorkflowKey {
	return storage.WorkflowKey{Namespace: "default", WorkflowID: workflowID, RunID: runID}
}

func (f *fixture) workflow(runKey storage.WorkflowKey, status temporalessv1.WorkflowStatus, created time.Duration, completed *time.Duration) *temporalessv1.WorkflowRecord {
	f.t.Helper()
	input, err := anypb.New(wrapperspb.String("input:" + runKey.RunID))
	if err != nil {
		f.t.Fatal(err)
	}
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key:           runKey.Proto(),
		WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
		Input:         input,
		Status:        status,
		CreatedAt:     at(created),
		Annotations:   map[string]string{"feed": runKey.WorkflowID},
	}
	if completed != nil {
		record.CompletedAt = at(*completed)
		if status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
			record.Failure = &temporalessv1.ActivityFailure{Code: "upstream_5xx", Message: "bad gateway"}
		} else {
			result, err := anypb.New(wrapperspb.String("done"))
			if err != nil {
				f.t.Fatal(err)
			}
			record.Result = result
		}
	}
	if err := f.records.PutWorkflow(context.Background(), record); err != nil {
		f.t.Fatal(err)
	}
	return record
}

func (f *fixture) activity(runKey storage.WorkflowKey, id string, status temporalessv1.ActivityStatus, attempts []*temporalessv1.ActivityAttempt, nextAttempt *timestamppb.Timestamp) {
	f.t.Helper()
	record := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key:           storage.ActivityKey{Namespace: runKey.Namespace, WorkflowID: runKey.WorkflowID, RunID: runKey.RunID, ActivityID: id}.Proto(),
		ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
		Status:        status,
		Attempts:      attempts,
		NextAttemptAt: nextAttempt,
		RetryPolicy: &temporalessv1.RetryPolicy{
			InitialInterval:    durationpb.New(30 * time.Second),
			BackoffCoefficient: 2,
			MaximumAttempts:    6,
		},
	}
	if len(attempts) > 0 {
		record.CreatedAt = attempts[0].GetCompletedAt()
	}
	switch status {
	case temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED:
		record.CompletedAt = attempts[len(attempts)-1].GetCompletedAt()
		result, err := anypb.New(wrapperspb.String("ok:" + id))
		if err != nil {
			f.t.Fatal(err)
		}
		record.Result = result
	case temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED:
		record.CompletedAt = attempts[len(attempts)-1].GetCompletedAt()
		record.Failure = attempts[len(attempts)-1].GetFailure()
	}
	if err := f.records.PutActivity(context.Background(), record); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) timer(runKey storage.WorkflowKey, id string, kind temporalessv1.TimerKind, status temporalessv1.TimerStatus, fireAt time.Duration) *temporalessv1.TimerRecord {
	f.t.Helper()
	record := &temporalessv1.TimerRecord{
		SchemaVersion: storage.TimerRecordSchemaVersion,
		Key:           storage.TimerKey{Namespace: runKey.Namespace, WorkflowID: runKey.WorkflowID, RunID: runKey.RunID, TimerID: id}.Proto(),
		TimerKind:     kind,
		Duration:      durationpb.New(4 * time.Minute),
		Status:        status,
		FireAt:        at(fireAt),
		CreatedAt:     at(fireAt - 4*time.Minute),
	}
	if kind == temporalessv1.TimerKind_TIMER_KIND_ACTIVITY_RETRY {
		record.RetryActivityId = "fetch:page-3"
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		record.FiredAt = at(fireAt + 5*time.Second)
	}
	if err := f.records.PutTimer(context.Background(), record); err != nil {
		f.t.Fatal(err)
	}
	return record
}

// claim writes a claim record at its canonical key. The Go OpenDAL point
// store deliberately exposes no claim writes, so the fixture writes bytes.
func (f *fixture) claim(runKey storage.WorkflowKey, id string, resource temporalessv1.ClaimResourceType, leaseExpires time.Duration) {
	f.t.Helper()
	claimKey := storage.ClaimKey{Namespace: runKey.Namespace, WorkflowID: runKey.WorkflowID, RunID: runKey.RunID, ClaimID: id}
	record := &temporalessv1.ClaimRecord{
		SchemaVersion:  storage.ClaimRecordSchemaVersion,
		Key:            claimKey.Proto(),
		OwnerId:        "worker-7",
		ResourceType:   resource,
		ResourceId:     runKey.RunID,
		LeaseExpiresAt: at(leaseExpires),
		CreatedAt:      at(leaseExpires - 5*time.Minute),
	}
	f.write(mustPath(f.t, claimKey.Path), mustMarshal(f.t, record))
}

func (f *fixture) event(runKey storage.WorkflowKey, id string, received time.Duration) {
	f.t.Helper()
	payload, err := anypb.New(wrapperspb.Bool(true))
	if err != nil {
		f.t.Fatal(err)
	}
	record := &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           storage.EventKey{Namespace: runKey.Namespace, WorkflowID: runKey.WorkflowID, RunID: runKey.RunID, EventID: id}.Proto(),
		Payload:       payload,
		ReceivedAt:    at(received),
	}
	if err := f.records.PutEvent(context.Background(), record); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) write(path string, data []byte) {
	f.t.Helper()
	full := filepath.Join(f.root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) remove(path string) {
	f.t.Helper()
	if err := os.Remove(filepath.Join(f.root, filepath.FromSlash(path))); err != nil {
		f.t.Fatal(err)
	}
}

// snapshot fingerprints every file under the store root, so a test can prove
// inspection wrote, repaired, and deleted nothing.
func (f *fixture) snapshot() map[string][32]byte {
	f.t.Helper()
	files := make(map[string][32]byte)
	err := filepath.WalkDir(f.root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[path] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return files
}

func mustPath(t *testing.T, path func() (string, error)) string {
	t.Helper()
	value, err := path()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func duration(value time.Duration) *time.Duration { return &value }

func attempt(number uint32, start, end time.Duration, failureCode string) *temporalessv1.ActivityAttempt {
	record := &temporalessv1.ActivityAttempt{Attempt: number, StartedAt: at(start), CompletedAt: at(end)}
	if failureCode != "" {
		record.Failure = &temporalessv1.ActivityFailure{Code: failureCode, Message: failureCode + " from upstream"}
	}
	return record
}
