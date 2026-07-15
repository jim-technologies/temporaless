package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRetryingActivityRejectsInconsistentPersistedState(t *testing.T) {
	tests := []struct {
		name   string
		policy *temporalessv1.RetryPolicy
		mutate func(*temporalessv1.ActivityRecord)
	}{
		{
			name: "missing top-level failure",
			policy: &temporalessv1.RetryPolicy{
				InitialInterval:    durationpb.New(time.Minute),
				MaximumAttempts:    3,
				BackoffCoefficient: 1,
			},
			mutate: func(record *temporalessv1.ActivityRecord) {
				record.Failure = nil
			},
		},
		{
			name: "top-level failure differs from last attempt",
			policy: &temporalessv1.RetryPolicy{
				InitialInterval:    durationpb.New(time.Minute),
				MaximumAttempts:    3,
				BackoffCoefficient: 1,
			},
			mutate: func(record *temporalessv1.ActivityRecord) {
				record.Failure = &temporalessv1.ActivityFailure{
					Code:    "different",
					Message: "not the last attempt failure",
				}
			},
		},
		{
			name: "last failure is non-retryable",
			policy: &temporalessv1.RetryPolicy{
				InitialInterval:        durationpb.New(time.Minute),
				MaximumAttempts:        3,
				BackoffCoefficient:     1,
				NonRetryableErrorCodes: []string{"fatal"},
			},
			mutate: func(record *temporalessv1.ActivityRecord) {
				failure := &temporalessv1.ActivityFailure{Code: "fatal", Message: "do not retry"}
				record.Failure = proto.Clone(failure).(*temporalessv1.ActivityFailure)
				record.Attempts[0].Failure = failure
			},
		},
		{
			name: "durable interval omits next attempt and timer ID",
			policy: &temporalessv1.RetryPolicy{
				InitialInterval:         durationpb.New(time.Hour),
				MaximumAttempts:         3,
				BackoffCoefficient:      1,
				DurableBackoffThreshold: durationpb.New(time.Second),
			},
			mutate: func(record *temporalessv1.ActivityRecord) {
				record.NextAttemptAt = nil
				record.RetryTimerId = ""
			},
		},
		{
			name: "retry-after durable interval omits next attempt and timer ID",
			policy: &temporalessv1.RetryPolicy{
				InitialInterval:         durationpb.New(time.Second),
				MaximumAttempts:         3,
				BackoffCoefficient:      1,
				DurableBackoffThreshold: durationpb.New(time.Minute),
			},
			mutate: func(record *temporalessv1.ActivityRecord) {
				record.Failure.RetryAfter = durationpb.New(time.Hour)
				record.Attempts[0].Failure.RetryAfter = durationpb.New(time.Hour)
				record.NextAttemptAt = nil
				record.RetryTimerId = ""
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			workflow := &Workflow{
				store:       store,
				workflowID:  "record-invariant",
				runID:       fmt.Sprintf("retrying-%d", index),
				codeVersion: "v1",
			}
			plan, err := planRetries(test.policy)
			if err != nil {
				t.Fatal(err)
			}
			input, err := anypb.New(wrapperspb.String("request"))
			if err != nil {
				t.Fatal(err)
			}
			now := timestamppb.Now()
			failure := &temporalessv1.ActivityFailure{Code: "transient", Message: "try again"}
			record := &temporalessv1.ActivityRecord{
				SchemaVersion: storage.ActivityRecordSchemaVersion,
				Key: storage.NewActivityKey(
					workflow.workflowID,
					workflow.runID,
					"act",
				).Proto(),
				ActivityType: activityClaimTestType,
				CodeVersion:  workflow.codeVersion,
				Input:        input,
				Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING,
				Failure:      proto.Clone(failure).(*temporalessv1.ActivityFailure),
				CreatedAt:    now,
				Attempts: []*temporalessv1.ActivityAttempt{
					{
						Attempt:     1,
						StartedAt:   now,
						CompletedAt: now,
						Failure:     failure,
					},
				},
				RetryPolicy: normalizeRetryPolicy(test.policy, plan),
			}
			test.mutate(record)
			if err := store.PutActivity(ctx, record); err != nil {
				t.Fatal(err)
			}

			executions := 0
			_, err = runActivity(
				ctx,
				workflow,
				"act",
				activityClaimTestType,
				test.policy,
				testRetryTimerID("act"),
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context) (*wrapperspb.StringValue, error) {
					executions++
					return wrapperspb.String("unexpected"), nil
				},
			)
			if !errors.Is(err, ErrActivityConflict) {
				t.Fatalf("error = %v, want activity conflict", err)
			}
			if executions != 0 {
				t.Fatalf("activity executions = %d, want 0", executions)
			}
		})
	}
}

type malformedTimerReadStore struct {
	storage.Store
	key    storage.TimerKey
	record *temporalessv1.TimerRecord
}

func (store *malformedTimerReadStore) GetTimer(
	ctx context.Context,
	key storage.TimerKey,
) (*temporalessv1.TimerRecord, bool, error) {
	if key == store.key {
		return proto.Clone(store.record).(*temporalessv1.TimerRecord), true, nil
	}
	return store.Store.GetTimer(ctx, key)
}

func TestSleepRejectsMalformedPersistedTimer(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		mutate   func(*temporalessv1.TimerRecord)
	}{
		{
			name:     "missing duration",
			duration: 0,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Duration = nil
			},
		},
		{
			name:     "non-canonical duration",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Duration = &durationpb.Duration{Nanos: 1_000_000_000}
			},
		},
		{
			name:     "negative duration",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Duration = durationpb.New(-time.Second)
			},
		},
		{
			name:     "invalid fire-at",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.FireAt = &timestamppb.Timestamp{
					Seconds: time.Now().UTC().Add(time.Hour).Unix(),
					Nanos:   1_000_000_000,
				}
			},
		},
		{
			name:     "scheduled timer has fired-at",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Status = temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED
				record.FiredAt = timestamppb.Now()
			},
		},
		{
			name:     "fired timer has no fired-at",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
				record.FiredAt = nil
			},
		},
		{
			name:     "fired timer has invalid fired-at",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
				record.FiredAt = &timestamppb.Timestamp{
					Seconds: time.Now().UTC().Unix(),
					Nanos:   1_000_000_000,
				}
			},
		},
		{
			name:     "fired timer has no fire-at",
			duration: time.Second,
			mutate: func(record *temporalessv1.TimerRecord) {
				record.Status = temporalessv1.TimerStatus_TIMER_STATUS_FIRED
				record.FireAt = nil
				record.FiredAt = timestamppb.Now()
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := storage.NewTimerKey("record-invariant", fmt.Sprintf("sleep-%d", index), "wait")
			now := time.Now().UTC()
			record := &temporalessv1.TimerRecord{
				SchemaVersion: storage.TimerRecordSchemaVersion,
				Key:           key.Proto(),
				TimerKind:     temporalessv1.TimerKind_TIMER_KIND_SLEEP,
				CodeVersion:   "v1",
				Duration:      durationpb.New(test.duration),
				Status:        temporalessv1.TimerStatus_TIMER_STATUS_SCHEDULED,
				FireAt:        timestamppb.New(now.Add(time.Hour)),
				CreatedAt:     timestamppb.New(now),
			}
			test.mutate(record)
			store := &malformedTimerReadStore{
				Store:  newTestStore(t),
				key:    key,
				record: record,
			}
			workflow := &Workflow{
				store:       store,
				workflowID:  key.WorkflowID,
				runID:       key.RunID,
				codeVersion: "v1",
			}
			ctx := context.WithValue(context.Background(), workflowContextKey{}, workflow)

			err := Sleep(ctx, key.TimerID, test.duration)
			if !errors.Is(err, ErrTimerConflict) {
				t.Fatalf("error = %v, want timer conflict", err)
			}
		})
	}
}

func TestSleepRejectsNegativeDurationWithoutWritingTimer(t *testing.T) {
	store := newTestStore(t)
	workflow := &Workflow{
		store:       store,
		workflowID:  "record-invariant",
		runID:       "negative-sleep",
		codeVersion: "v1",
	}
	ctx := context.WithValue(context.Background(), workflowContextKey{}, workflow)
	key := storage.NewTimerKey(workflow.workflowID, workflow.runID, "wait")

	err := Sleep(ctx, key.TimerID, -time.Nanosecond)
	if err == nil {
		t.Fatal("Sleep() error = nil, want negative-duration rejection")
	}
	if _, found, getErr := store.GetTimer(context.Background(), key); getErr != nil {
		t.Fatal(getErr)
	} else if found {
		t.Fatal("negative-duration Sleep() wrote a timer")
	}
}

func TestTerminalFailedActivityRequiresFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	workflow := &Workflow{
		store:       store,
		workflowID:  "record-invariant",
		runID:       "failed-activity",
		codeVersion: "v1",
	}
	input, err := anypb.New(wrapperspb.String("request"))
	if err != nil {
		t.Fatal(err)
	}
	now := timestamppb.Now()
	if err := store.PutActivity(ctx, &temporalessv1.ActivityRecord{
		SchemaVersion: storage.ActivityRecordSchemaVersion,
		Key: storage.NewActivityKey(
			workflow.workflowID,
			workflow.runID,
			"act",
		).Proto(),
		ActivityType: activityClaimTestType,
		CodeVersion:  workflow.codeVersion,
		Input:        input,
		Status:       temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED,
		CreatedAt:    now,
		CompletedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	_, err = runActivity(
		ctx,
		workflow,
		"act",
		activityClaimTestType,
		nil,
		"",
		wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrActivityConflict) {
		t.Fatalf("error = %v, want activity conflict", err)
	}
	if executions != 0 {
		t.Fatalf("activity executions = %d, want 0", executions)
	}
}

func TestTerminalFailedWorkflowRequiresFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	options := &Options{
		WorkflowId:  "record-invariant",
		RunId:       "failed-workflow",
		CodeVersion: "v1",
	}
	input := wrapperspb.String("request")
	inputAny, err := anypb.New(input)
	if err != nil {
		t.Fatal(err)
	}
	now := timestamppb.Now()
	if err := store.PutWorkflow(ctx, &temporalessv1.WorkflowRecord{
		SchemaVersion: storage.WorkflowRecordSchemaVersion,
		Key: storage.NewWorkflowKey(
			options.GetWorkflowId(),
			options.GetRunId(),
		).Proto(),
		WorkflowType: messagePairType("workflow", input, &wrapperspb.StringValue{}),
		CodeVersion:  options.GetCodeVersion(),
		Input:        inputAny,
		Status:       temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED,
		CreatedAt:    now,
		CompletedAt:  now,
	}); err != nil {
		t.Fatal(err)
	}

	executions := 0
	_, err = Run(
		ctx,
		store,
		options,
		nil,
		input,
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrWorkflowConflict) {
		t.Fatalf("error = %v, want workflow conflict", err)
	}
	if executions != 0 {
		t.Fatalf("workflow executions = %d, want 0", executions)
	}
}
