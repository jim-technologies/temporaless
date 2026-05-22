package storage_test

import (
	"context"
	"fmt"
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

// BenchmarkListWorkflowsScan measures the cost of walking the workflow tree.
// Run once with N workflow runs already populated; subreports per-record cost.
func BenchmarkListWorkflowsScan(b *testing.B) {
	tests := []int{10, 100, 500}
	for _, n := range tests {
		b.Run(fmt.Sprintf("workflows=%d", n), func(b *testing.B) {
			store := newBenchStore(b)
			ctx := context.Background()
			for i := 0; i < n; i++ {
				if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
					SchemaVersion: storage.WorkflowRecordSchemaVersion,
					Key: storage.WorkflowKey{
						Namespace:  storage.DefaultNamespace,
						WorkflowID: "bench:wf",
						RunID:      fmt.Sprintf("run-%05d", i),
					}.Proto(),
					WorkflowType: "test:type",
					CodeVersion:  "v1",
					Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
				}); err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				records, err := store.ListWorkflows(ctx, storage.DefaultNamespace, "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
				if err != nil {
					b.Fatal(err)
				}
				if len(records) != n {
					b.Fatalf("records = %d, want %d", len(records), n)
				}
			}
		})
	}
}

// BenchmarkListWorkflowsScopedByID measures the cost of listing one schedule's
// runs vs. the unscoped variant. Demonstrates the value of the workflow_id filter.
func BenchmarkListWorkflowsScopedByID(b *testing.B) {
	const totalSchedules = 50
	const runsPerSchedule = 10

	store := newBenchStore(b)
	ctx := context.Background()
	for s := 0; s < totalSchedules; s++ {
		for r := 0; r < runsPerSchedule; r++ {
			if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
				SchemaVersion: storage.WorkflowRecordSchemaVersion,
				Key: storage.WorkflowKey{
					Namespace:  storage.DefaultNamespace,
					WorkflowID: fmt.Sprintf("schedule-%03d", s),
					RunID:      fmt.Sprintf("run-%05d", r),
				}.Proto(),
				WorkflowType: "test:type",
				CodeVersion:  "v1",
				Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			}); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("unscoped", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			records, err := store.ListWorkflows(ctx, storage.DefaultNamespace, "", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
			if err != nil {
				b.Fatal(err)
			}
			if len(records) != totalSchedules*runsPerSchedule {
				b.Fatalf("unscoped count = %d, want %d", len(records), totalSchedules*runsPerSchedule)
			}
		}
	})

	b.Run("scoped_by_workflow_id", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			records, err := store.ListWorkflows(ctx, storage.DefaultNamespace, "schedule-025", temporalessv1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
			if err != nil {
				b.Fatal(err)
			}
			if len(records) != runsPerSchedule {
				b.Fatalf("scoped count = %d, want %d", len(records), runsPerSchedule)
			}
		}
	})
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
