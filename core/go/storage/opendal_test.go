package storage

import (
	"context"
	"testing"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestOpenDALStoreRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *OpenDALStore, *anypb.Any) (bool, string, error)
	}{
		{
			name: "workflow",
			run: func(ctx context.Context, store *OpenDALStore, result *anypb.Any) (bool, string, error) {
				key := NewWorkflowKey("prices:aapl", "2026-05-02")
				record := &temporalessv1.WorkflowRecord{
					SchemaVersion: WorkflowRecordSchemaVersion,
					Key:           key.Proto(),
					WorkflowType:  "workflow:google.protobuf.StringValue->google.protobuf.StringValue",
					CodeVersion:   "test-version",
					Status:        temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
					Result:        result,
				}
				if err := store.PutWorkflow(ctx, record); err != nil {
					return false, "", err
				}
				got, found, err := store.GetWorkflow(ctx, key)
				if err != nil {
					return false, "", err
				}
				return found, got.GetResult().GetTypeUrl(), nil
			},
		},
		{
			name: "activity",
			run: func(ctx context.Context, store *OpenDALStore, result *anypb.Any) (bool, string, error) {
				key := NewActivityKey("prices:aapl", "2026-05-02", "fetch:price")
				record := &temporalessv1.ActivityRecord{
					SchemaVersion: ActivityRecordSchemaVersion,
					Key:           key.Proto(),
					ActivityType:  "activity:google.protobuf.StringValue->google.protobuf.StringValue",
					CodeVersion:   "test-version",
					Status:        temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED,
					Result:        result,
				}
				if err := store.PutActivity(ctx, record); err != nil {
					return false, "", err
				}
				got, found, err := store.GetActivity(ctx, key)
				if err != nil {
					return false, "", err
				}
				return found, got.GetResult().GetTypeUrl(), nil
			},
		},
		{
			name: "timer",
			run: func(ctx context.Context, store *OpenDALStore, _ *anypb.Any) (bool, string, error) {
				key := NewTimerKey("prices:aapl", "2026-05-02", "wait:vendor-window")
				record := &temporalessv1.TimerRecord{
					SchemaVersion: TimerRecordSchemaVersion,
					Key:           key.Proto(),
					TimerKind:     SleepTimerKind,
					CodeVersion:   "test-version",
					Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
					FireAt:        timestamppb.Now(),
				}
				if err := store.PutTimer(ctx, record); err != nil {
					return false, "", err
				}
				got, found, err := store.GetTimer(ctx, key)
				if err != nil {
					return false, "", err
				}
				return found, got.GetTimerKind().String(), nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newOpenDALTestStore(t)
			result, err := anypb.New(wrapperspb.String("100.00"))
			if err != nil {
				t.Fatal(err)
			}

			found, typeURL, err := test.run(context.Background(), store, result)
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("record not found")
			}
			wantType := result.GetTypeUrl()
			if test.name == "timer" {
				wantType = SleepTimerKind.String()
			}
			if typeURL != wantType {
				t.Fatalf("result type = %q, want %q", typeURL, wantType)
			}
		})
	}
}

func newOpenDALTestStore(t *testing.T) *OpenDALStore {
	t.Helper()

	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return NewOpenDALStore(operator)
}
