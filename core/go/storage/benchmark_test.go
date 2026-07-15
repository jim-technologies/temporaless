package storage_test

import (
	"context"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// BenchmarkPutGetWorkflow measures the round-trip cost of writing and reading
// a workflow record through the OpenDAL fs backend. This is the hot path for
// workflow.Run on each invocation.
func BenchmarkPutGetWorkflow(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()
	resultAny, err := anypb.New(wrapperspb.String("benchmark-result"))
	if err != nil {
		b.Fatal(err)
	}
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key: storage.WorkflowKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: "bench:wf",
			RunID:      "run",
		}.Proto(),
		WorkflowType: "test:type",
		CodeVersion:  "v1",
		Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		Result:       resultAny,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.PutWorkflow(ctx, record); err != nil {
			b.Fatal(err)
		}
		if _, _, err := store.GetWorkflow(ctx, storage.WorkflowKeyFromProto(record.GetKey())); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutGetActivity measures the per-activity-record round-trip cost.
func BenchmarkPutGetActivity(b *testing.B) {
	store := newBenchStore(b)
	ctx := context.Background()
	resultAny, err := anypb.New(wrapperspb.String("benchmark-result"))
	if err != nil {
		b.Fatal(err)
	}
	record := &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.ActivityKey{
			Namespace:  storage.DefaultNamespace,
			WorkflowID: "bench:wf",
			RunID:      "run",
			ActivityID: "fetch",
		}.Proto(),
		ActivityType: "test:activity",
		CodeVersion:  "v1",
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
		Result:       resultAny,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.PutActivity(ctx, record); err != nil {
			b.Fatal(err)
		}
		if _, _, err := store.GetActivity(ctx, storage.ActivityKeyFromProto(record.GetKey())); err != nil {
			b.Fatal(err)
		}
	}
}

func newBenchStore(b *testing.B) *storage.OpenDALStore {
	b.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}
