package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newDueTimerTestStore(t *testing.T) (*OpenDALStore, *opendal.Operator) {
	t.Helper()
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return NewOpenDALStore(operator), operator
}

func putDueTimerTestWorkflow(
	t *testing.T,
	store *OpenDALStore,
	key WorkflowKey,
	status temporalessv1.WorkflowStatus,
	now time.Time,
) {
	t.Helper()
	record := &temporalessv1.WorkflowRecord{
		SchemaVersion: WorkflowRecordSchemaVersion,
		Key:           key.Proto(),
		WorkflowType:  "workflow:test",
		CodeVersion:   "v1",
		Status:        status,
		CreatedAt:     timestamppb.New(now.Add(-time.Minute)),
	}
	if status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED ||
		status == temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		record.CompletedAt = timestamppb.New(now)
	}
	if err := store.PutWorkflow(context.Background(), record); err != nil {
		t.Fatal(err)
	}
}

func dueTimerTestRecord(key TimerKey, fireAt time.Time, status temporalessv1.TimerStatus) *temporalessv1.TimerRecord {
	record := &temporalessv1.TimerRecord{
		SchemaVersion: TimerRecordSchemaVersion,
		Key:           key.Proto(),
		TimerKind:     SleepTimerKind,
		CodeVersion:   "v1",
		Duration:      durationpb.New(time.Minute),
		Status:        status,
		FireAt:        timestamppb.New(fireAt),
		CreatedAt:     timestamppb.New(fireAt.Add(-time.Minute)),
	}
	if status == temporalessv1.TimerStatus_TIMER_STATUS_FIRED {
		record.FiredAt = timestamppb.New(fireAt)
	}
	return record
}

func writeDueTimerTestBytes(t *testing.T, operator *opendal.Operator, path string, data []byte) {
	t.Helper()
	lastSlash := -1
	for index := len(path) - 1; index >= 0; index-- {
		if path[index] == '/' {
			lastSlash = index
			break
		}
	}
	if lastSlash < 0 {
		t.Fatalf("path %q has no directory", path)
	}
	if err := operator.CreateDir(path[:lastSlash+1]); err != nil {
		t.Fatal(err)
	}
	if err := operator.Write(path, data); err != nil {
		t.Fatal(err)
	}
}

func TestOpenDALStoreDueTimerLedgerLifecycle(t *testing.T) {
	ctx := context.Background()
	store, operator := newDueTimerTestStore(t)
	now := time.Date(2030, time.January, 1, 0, 0, 0, 123456000, time.UTC)
	workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
	timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "wake"}
	putDueTimerTestWorkflow(t, store, workflowKey, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)

	record := dueTimerTestRecord(timerKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}
	ledgerPath, err := dueEntryPath(timerKey)
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := operator.IsExist(ledgerPath); err != nil || !exists {
		t.Fatalf("initial due entry exists = %v, err = %v", exists, err)
	}
	for _, namespace := range []string{"alpha", ""} {
		due, err := store.DueTimers(ctx, namespace, now)
		if err != nil || len(due) != 1 || due[0].Key != timerKey {
			t.Fatalf("DueTimers(%q) = %+v, err=%v", namespace, due, err)
		}
	}

	record.FireAt = timestamppb.New(now.Add(time.Hour))
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}
	if due, err := store.DueTimers(ctx, "alpha", now); err != nil || len(due) != 0 {
		t.Fatalf("before replacement fire_at: due=%+v err=%v", due, err)
	}

	record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
	record.FiredAt = proto.Clone(record.GetFireAt()).(*timestamppb.Timestamp)
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}
	if due, err := store.DueTimers(ctx, "alpha", now.Add(2*time.Hour)); err != nil || len(due) != 0 {
		t.Fatalf("fired timer dispatched: due=%+v err=%v", due, err)
	}

	record.Status = temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
	record.FiredAt = nil
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}
	deleted, err := store.DeleteTimer(ctx, timerKey)
	if err != nil || !deleted {
		t.Fatalf("DeleteTimer deleted=%v err=%v", deleted, err)
	}
	if _, found, err := store.GetTimer(ctx, timerKey); err != nil || found {
		t.Fatalf("deleted timer fallback: found=%v err=%v", found, err)
	}
	entry, found, err := store.getDueEntry(ctx, timerKey)
	if err != nil || !found || entry.GetRecord().GetStatus() != temporalessv1.TimerStatus_TIMER_STATUS_CANCELED {
		t.Fatalf("delete tombstone: entry=%v found=%v err=%v", entry, found, err)
	}
}

func TestOpenDALStoreDueTimersQuarantinesAndReportsInvalidEntries(t *testing.T) {
	tests := []struct {
		name  string
		write func(*testing.T, *opendal.Operator, WorkflowKey, time.Time) string
	}{
		{
			name: "undecodable protobuf",
			write: func(t *testing.T, operator *opendal.Operator, _ WorkflowKey, _ time.Time) string {
				path := dueRoot("alpha") + "workflow/run/zzz-undecodable.binpb"
				writeDueTimerTestBytes(t, operator, path, []byte("not protobuf"))
				return path
			},
		},
		{
			name: "misplaced payload",
			write: func(t *testing.T, operator *opendal.Operator, workflowKey WorkflowKey, now time.Time) string {
				timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "actual"}
				record := dueTimerTestRecord(timerKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&temporalessv1.DueTimerEntry{
					Key: timerKey.Proto(), WorkflowKey: workflowKey.Proto(), FireAt: record.GetFireAt(), Record: record,
				})
				if err != nil {
					t.Fatal(err)
				}
				path := dueRoot("alpha") + "workflow/run/zzz-misplaced.binpb"
				writeDueTimerTestBytes(t, operator, path, data)
				return path
			},
		},
		{
			name: "cross-run payload",
			write: func(t *testing.T, operator *opendal.Operator, _ WorkflowKey, now time.Time) string {
				timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "zzz-cross-run"}
				record := dueTimerTestRecord(timerKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&temporalessv1.DueTimerEntry{
					Key: timerKey.Proto(),
					WorkflowKey: (&WorkflowKey{
						Namespace: "alpha", WorkflowID: "workflow", RunID: "other-run",
					}).Proto(),
					FireAt: record.GetFireAt(),
					Record: record,
				})
				if err != nil {
					t.Fatal(err)
				}
				path, err := dueEntryPath(timerKey)
				if err != nil {
					t.Fatal(err)
				}
				writeDueTimerTestBytes(t, operator, path, data)
				return path
			},
		},
		{
			name: "missing prepared record",
			write: func(t *testing.T, operator *opendal.Operator, workflowKey WorkflowKey, now time.Time) string {
				timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "zzz-missing-record"}
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(&temporalessv1.DueTimerEntry{
					Key: timerKey.Proto(), WorkflowKey: workflowKey.Proto(), FireAt: timestamppb.New(now),
				})
				if err != nil {
					t.Fatal(err)
				}
				path, err := dueEntryPath(timerKey)
				if err != nil {
					t.Fatal(err)
				}
				writeDueTimerTestBytes(t, operator, path, data)
				return path
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, operator := newDueTimerTestStore(t)
			now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
			workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
			validKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "aaa-valid"}
			putDueTimerTestWorkflow(t, store, workflowKey, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
			if err := store.PutTimer(ctx, dueTimerTestRecord(validKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)); err != nil {
				t.Fatal(err)
			}
			source := test.write(t, operator, workflowKey, now)

			due, err := store.DueTimers(ctx, "alpha", now)
			if len(due) != 0 || !errors.Is(err, ErrCorruptRecord) {
				t.Fatalf("due=%+v err=%v, want no partial result and ErrCorruptRecord", due, err)
			}
			if exists, err := operator.IsExist(source); err != nil || !exists {
				t.Fatalf("invalid source retained=%v err=%v", exists, err)
			}
			quarantined, err := walkOpenDAL(ctx, operator, dueInvalidRoot("alpha"))
			if err != nil || len(quarantined) != 1 {
				t.Fatalf("quarantined=%v err=%v, want one diagnostic copy", quarantined, err)
			}
		})
	}
}

func TestOpenDALStoreDueLedgerWinsAfterOverwriteCrash(t *testing.T) {
	ctx := context.Background()
	store, operator := newDueTimerTestStore(t)
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)

	terminalKey := TimerKey{Namespace: "alpha", WorkflowID: "terminal", RunID: "run", TimerID: "wake"}
	terminalWorkflow := WorkflowKey{Namespace: "alpha", WorkflowID: "terminal", RunID: "run"}
	putDueTimerTestWorkflow(t, store, terminalWorkflow, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
	if err := store.PutTimer(ctx, dueTimerTestRecord(terminalKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)); err != nil {
		t.Fatal(err)
	}
	putDueTimerTestWorkflow(t, store, terminalWorkflow, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, now)

	mismatchKey := TimerKey{Namespace: "alpha", WorkflowID: "mismatch", RunID: "run", TimerID: "wake"}
	putDueTimerTestWorkflow(t, store, WorkflowKey{Namespace: "alpha", WorkflowID: "mismatch", RunID: "run"}, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
	prepared := dueTimerTestRecord(mismatchKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	if _, err := store.putDueEntry(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	point := dueTimerTestRecord(mismatchKey, now.Add(time.Hour), temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	pointPath, err := mismatchKey.Path()
	if err != nil {
		t.Fatal(err)
	}
	pointData, err := proto.MarshalOptions{Deterministic: true}.Marshal(point)
	if err != nil {
		t.Fatal(err)
	}
	writeDueTimerTestBytes(t, operator, pointPath, pointData)

	replayed, found, err := store.GetTimer(ctx, mismatchKey)
	if err != nil || !found || !proto.Equal(replayed, prepared) {
		t.Fatalf("overwrite fallback=%v found=%v err=%v", replayed, found, err)
	}
	listed, err := store.ListTimers(
		ctx,
		WorkflowKey{Namespace: "alpha", WorkflowID: "mismatch", RunID: "run"},
		temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED,
	)
	if err != nil || len(listed) != 1 || !proto.Equal(listed[0], prepared) {
		t.Fatalf("overwrite list=%v err=%v", listed, err)
	}
	due, err := store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 0 {
		t.Fatalf("first overwrite scan due=%+v err=%v, want repair only", due, err)
	}
	due, err = store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 1 || due[0].Key != mismatchKey || !proto.Equal(due[0].Record, prepared) {
		t.Fatalf("repaired overwrite due=%+v err=%v", due, err)
	}
}

func TestOpenDALStoreListTimersRecoversCorruptPointOnlyFromExactDueShadow(t *testing.T) {
	tests := []struct {
		name         string
		pointPayload string
		shadow       string
		shadowStatus temporalessv1.TimerStatus
		wantRecord   bool
		wantErr      bool
	}{
		{
			name:         "undecodable point with scheduled shadow",
			pointPayload: "undecodable",
			shadow:       "exact",
			shadowStatus: temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			wantRecord:   true,
		},
		{
			name:         "misplaced point with scheduled shadow",
			pointPayload: "misplaced",
			shadow:       "exact",
			shadowStatus: temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			wantRecord:   true,
		},
		{
			name:         "undecodable point with tombstone shadow",
			pointPayload: "undecodable",
			shadow:       "exact",
			shadowStatus: temporalessv1.TimerStatus_TIMER_STATUS_CANCELED,
		},
		{
			name:         "undecodable point without shadow",
			pointPayload: "undecodable",
			shadow:       "none",
			wantErr:      true,
		},
		{
			name:         "undecodable point with different shadow",
			pointPayload: "undecodable",
			shadow:       "different",
			shadowStatus: temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
			wantErr:      true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, operator := newDueTimerTestStore(t)
			now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
			workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
			timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "wake"}
			prepared := dueTimerTestRecord(timerKey, now, test.shadowStatus)
			switch test.shadow {
			case "exact":
				if _, err := store.putDueEntry(ctx, prepared); err != nil {
					t.Fatal(err)
				}
			case "different":
				otherKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "other"}
				if _, err := store.putDueEntry(ctx, dueTimerTestRecord(otherKey, now, test.shadowStatus)); err != nil {
					t.Fatal(err)
				}
			case "none":
			default:
				t.Fatalf("unknown shadow setup %q", test.shadow)
			}

			pointPath, err := timerKey.Path()
			if err != nil {
				t.Fatal(err)
			}
			var pointData []byte
			switch test.pointPayload {
			case "undecodable":
				pointData = []byte("not a timer protobuf")
			case "misplaced":
				otherKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "other-point"}
				pointData, err = proto.MarshalOptions{Deterministic: true}.Marshal(
					dueTimerTestRecord(otherKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED),
				)
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unknown point payload %q", test.pointPayload)
			}
			writeDueTimerTestBytes(t, operator, pointPath, pointData)

			listed, err := store.ListTimers(ctx, workflowKey, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
			if test.wantErr {
				if !errors.Is(err, ErrCorruptRecord) || listed != nil {
					t.Fatalf("listed=%v err=%v, want nil and ErrCorruptRecord", listed, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantRecord {
				if len(listed) != 1 || !proto.Equal(listed[0], prepared) {
					t.Fatalf("listed=%v, want exact prepared shadow %v", listed, prepared)
				}
				return
			}
			if len(listed) != 0 {
				t.Fatalf("listed=%v, want tombstoned timer hidden", listed)
			}
		})
	}
}

func TestOpenDALStoreDueScannerCompletesInterruptedTimerDelete(t *testing.T) {
	ctx := context.Background()
	store, operator := newDueTimerTestStore(t)
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
	timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "wake"}
	putDueTimerTestWorkflow(t, store, workflowKey, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
	record := dueTimerTestRecord(timerKey, now.Add(time.Hour), temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}

	// Simulate a process death after DeleteTimer published its prepared
	// tombstone but before it removed the canonical timer point.
	tombstone := proto.Clone(record).(*temporalessv1.TimerRecord)
	tombstone.Status = temporalessv1.TimerStatus_TIMER_STATUS_CANCELED
	if _, err := store.putDueEntry(ctx, tombstone); err != nil {
		t.Fatal(err)
	}
	pointPath, err := timerKey.Path()
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := operator.IsExist(pointPath); err != nil || !exists {
		t.Fatalf("precondition canonical point exists=%v err=%v", exists, err)
	}

	due, err := store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 0 {
		t.Fatalf("tombstoned timer due=%+v err=%v", due, err)
	}
	if exists, err := operator.IsExist(pointPath); err != nil || exists {
		t.Fatalf("canonical point after reconciliation exists=%v err=%v", exists, err)
	}
	if _, found, err := store.GetTimer(ctx, timerKey); err != nil || found {
		t.Fatalf("tombstoned timer found=%v err=%v", found, err)
	}
}

func TestOpenDALStoreLedgerFirstCrashPreservesExactTimer(t *testing.T) {
	ctx := context.Background()
	store, operator := newDueTimerTestStore(t)
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
	timerKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "wake"}
	putDueTimerTestWorkflow(t, store, workflowKey, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
	prepared := dueTimerTestRecord(timerKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)
	prepared.CodeVersion = "exact-version"
	prepared.Duration = durationpb.New(10 * 365 * 24 * time.Hour)

	ledgerPath, err := store.putDueEntry(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	pointPath, err := timerKey.Path()
	if err != nil {
		t.Fatal(err)
	}
	if exists, err := operator.IsExist(pointPath); err != nil || exists {
		t.Fatalf("canonical point exists=%v err=%v", exists, err)
	}
	replayed, found, err := store.GetTimer(ctx, timerKey)
	if err != nil || !found || !proto.Equal(replayed, prepared) {
		t.Fatalf("GetTimer fallback=%v found=%v err=%v", replayed, found, err)
	}
	listed, err := store.ListTimers(ctx, workflowKey, temporalessv1.TimerStatus_TIMER_STATUS_UNSPECIFIED)
	if err != nil || len(listed) != 1 || !proto.Equal(listed[0], prepared) {
		t.Fatalf("ListTimers fallback=%v err=%v", listed, err)
	}
	due, err := store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 0 {
		t.Fatalf("first ledger-first scan due=%v err=%v, want repair only", due, err)
	}
	due, err = store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 1 || !proto.Equal(due[0].Record, prepared) {
		t.Fatalf("repaired ledger-first due=%v err=%v", due, err)
	}
	if exists, err := operator.IsExist(ledgerPath); err != nil || !exists {
		t.Fatalf("prepared ledger exists=%v err=%v", exists, err)
	}
}

func TestOpenDALStoreDueTimersRecoversFromCorruptCanonicalPoint(t *testing.T) {
	ctx := context.Background()
	store, operator := newDueTimerTestStore(t)
	now := time.Date(2030, time.January, 1, 0, 0, 0, 0, time.UTC)
	workflowKey := WorkflowKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run"}
	validKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "valid"}
	corruptKey := TimerKey{Namespace: "alpha", WorkflowID: "workflow", RunID: "run", TimerID: "corrupt"}
	putDueTimerTestWorkflow(t, store, workflowKey, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS, now)
	if err := store.PutTimer(ctx, dueTimerTestRecord(validKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.putDueEntry(ctx, dueTimerTestRecord(corruptKey, now, temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED)); err != nil {
		t.Fatal(err)
	}
	corruptPath, err := corruptKey.Path()
	if err != nil {
		t.Fatal(err)
	}
	writeDueTimerTestBytes(t, operator, corruptPath, []byte("not a timer protobuf"))

	due, err := store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 1 || due[0].Key != validKey {
		t.Fatalf("first due=%+v err=%v, want valid timer while corrupt point is repaired", due, err)
	}
	due, err = store.DueTimers(ctx, "alpha", now)
	if err != nil || len(due) != 2 {
		t.Fatalf("repaired due=%+v err=%v, want both prepared timers", due, err)
	}
	recovered, found, err := store.GetTimer(ctx, corruptKey)
	if err != nil || !found || recovered.GetCodeVersion() != "v1" {
		t.Fatalf("corrupt point recovery=%v found=%v err=%v", recovered, found, err)
	}
}
