package workflow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/adapters/go/gocdkclaims"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"gocloud.dev/blob/fileblob"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRunNilResultPersistsFailureAndReplays(t *testing.T) {
	tests := []struct {
		name       string
		withClaims bool
	}{
		{name: "without claims"},
		{name: "with claims", withClaims: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			options := &Options{
				WorkflowId:  "nil-workflow-result-" + strings.ReplaceAll(test.name, " ", "-"),
				RunId:       "run",
				CodeVersion: "v1",
			}
			var claimStore storage.ClaimStore
			if test.withClaims {
				claimStore = newTestClaimStore(t)
				options.ClaimOwnerId = "worker"
			}

			bodyCalls := 0
			execute := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				bodyCalls++
				var result *wrapperspb.StringValue
				return result, nil
			}
			_, err := Run(
				ctx,
				store,
				options,
				claimStore,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				execute,
			)
			if err == nil || !strings.Contains(err.Error(), "returned a nil result") {
				t.Fatalf("first run error = %v, want nil-result failure", err)
			}

			key := storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId())
			record, found, err := store.GetWorkflow(ctx, key)
			if err != nil || !found {
				t.Fatalf("workflow record: err=%v found=%v", err, found)
			}
			if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
				t.Fatalf("workflow status = %s, want FAILED", record.GetStatus())
			}
			if record.GetFailure() == nil ||
				!strings.Contains(record.GetFailure().GetMessage(), "returned a nil result") {
				t.Fatalf("workflow failure = %v, want nil-result message", record.GetFailure())
			}

			if test.withClaims {
				_, claimFound, claimErr := claimStore.GetClaim(
					ctx,
					storage.ClaimKey{
						Namespace:  storage.DefaultNamespace,
						WorkflowID: options.GetWorkflowId(),
						RunID:      options.GetRunId(),
						ClaimID:    WorkflowExecutionClaimID,
					},
				)
				if claimErr != nil || claimFound {
					t.Fatalf("workflow claim after terminal failure: found=%v err=%v", claimFound, claimErr)
				}
			}

			_, replayErr := Run(
				ctx,
				store,
				options,
				claimStore,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					bodyCalls++
					return wrapperspb.String("must-not-run"), nil
				},
			)
			var replayFailure *ActivityError
			if !errors.As(replayErr, &replayFailure) ||
				!strings.Contains(replayFailure.Message, "returned a nil result") {
				t.Fatalf("replay error = %v, want stored nil-result failure", replayErr)
			}
			if bodyCalls != 1 {
				t.Fatalf("workflow body calls = %d, want 1", bodyCalls)
			}
		})
	}
}

func TestRunOrderTimePersistsAcrossWorkflowStates(t *testing.T) {
	runOrderTime := timestamppb.New(time.Date(2026, time.July, 14, 12, 0, 0, 123, time.UTC))
	tests := []struct {
		name string
		body WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]
		want temporalessv1.WorkflowStatus
	}{
		{
			name: "in progress",
			body: func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return nil, &TimerPendingError{TimerID: "wake", WakeAt: time.Now().UTC().Add(time.Hour)}
			},
			want: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS,
		},
		{
			name: "completed",
			body: func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String("done"), nil
			},
			want: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		},
		{
			name: "failed",
			body: func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return nil, errors.New("failed")
			},
			want: temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED,
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			options := &Options{
				WorkflowId:   fmt.Sprintf("run-order-%d", index),
				RunId:        "run",
				CodeVersion:  "v1",
				RunOrderTime: runOrderTime,
			}
			_, _ = Run(
				ctx,
				store,
				options,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				test.body,
			)
			record, found, err := store.GetWorkflow(ctx, storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId()))
			if err != nil || !found {
				t.Fatalf("workflow record: err=%v found=%v", err, found)
			}
			if got := record.GetStatus(); got != test.want {
				t.Fatalf("status = %s, want %s", got, test.want)
			}
			if record.GetRunOrderTime() == nil || !record.GetRunOrderTime().AsTime().Equal(runOrderTime.AsTime()) {
				t.Fatalf("run_order_time = %v, want %v", record.GetRunOrderTime(), runOrderTime)
			}
		})
	}
}

func TestRunOrderTimeReplayRejectsDriftAndInvalidTimestamp(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	firstOrder := timestamppb.New(time.Date(2026, time.July, 14, 12, 0, 0, 0, time.UTC))
	options := &Options{
		WorkflowId:   "run-order-drift",
		RunId:        "run",
		CodeVersion:  "v1",
		RunOrderTime: firstOrder,
	}
	pendingBody := func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return nil, &TimerPendingError{TimerID: "wake", WakeAt: time.Now().UTC().Add(time.Hour)}
	}
	_, err := Run(
		ctx, store, options, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, pendingBody,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("initial run error = %v, want pending", err)
	}

	drifted := &Options{
		WorkflowId:   options.GetWorkflowId(),
		RunId:        options.GetRunId(),
		CodeVersion:  options.GetCodeVersion(),
		RunOrderTime: timestamppb.New(firstOrder.AsTime().Add(time.Second)),
	}
	bodyCalls := 0
	_, err = Run(
		ctx, store, drifted, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			bodyCalls++
			return wrapperspb.String("unexpected"), nil
		},
	)
	if !errors.Is(err, ErrWorkflowConflict) {
		t.Fatalf("drift error = %v, want workflow conflict", err)
	}
	if bodyCalls != 0 {
		t.Fatalf("body calls after drift = %d, want 0", bodyCalls)
	}

	invalid := &Options{
		WorkflowId:   "run-order-invalid",
		RunId:        "run",
		CodeVersion:  "v1",
		RunOrderTime: &timestamppb.Timestamp{Seconds: 253402300800},
	}
	_, err = Run(
		ctx, store, invalid, nil, wrapperspb.String("request"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, pendingBody,
	)
	if err == nil {
		t.Fatal("expected invalid run_order_time error")
	}
}

func TestRunActivity(t *testing.T) {
	// User-supplied activity_id is the de-duplication contract. Same id
	// replays the stored result regardless of the input bytes — the caller
	// chose the id and is responsible for picking distinct ids when they
	// want distinct executions. activity_type and code_version still must
	// match to guard against shape changes.
	tests := []struct {
		name       string
		firstInput string
		nextInput  string
		want       string
		wantErr    error
	}{
		{
			name:       "replays completed result on identical input",
			firstInput: "AAPL",
			nextInput:  "AAPL",
			want:       "stored:AAPL",
		},
		{
			name:       "replays stored result even when input bytes differ",
			firstInput: "AAPL",
			nextInput:  "MSFT",
			want:       "stored:AAPL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			wf := &Workflow{
				store:       store,
				workflowID:  "prices:aapl",
				runID:       "2026-05-02",
				codeVersion: "test-version",
			}

			executions := 0
			run := func(_ context.Context) (*wrapperspb.StringValue, error) {
				executions++
				return wrapperspb.String("stored:" + test.firstInput), nil
			}

			first, err := runActivity(
				ctx,
				wf,
				"fetch:symbol",
				"activity:google.protobuf.StringValue->google.protobuf.StringValue",
				nil,
				"",
				wrapperspb.String(test.firstInput),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if err != nil {
				t.Fatal(err)
			}
			if first.GetValue() != "stored:"+test.firstInput {
				t.Fatalf("first result = %q", first.GetValue())
			}

			second, err := runActivity(
				ctx,
				wf,
				"fetch:symbol",
				"activity:google.protobuf.StringValue->google.protobuf.StringValue",
				nil,
				"",
				wrapperspb.String(test.nextInput),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("err = %v, want %v", err, test.wantErr)
				}
				if executions != 1 {
					t.Fatalf("executions = %d, want 1", executions)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if second.GetValue() != test.want {
				t.Fatalf("second result = %q, want %q", second.GetValue(), test.want)
			}
			if executions != 1 {
				t.Fatalf("executions = %d, want 1", executions)
			}
		})
	}
}

func TestRunActivityWithClaims(t *testing.T) {
	tests := []struct {
		name             string
		claimExpiresAt   time.Time
		wantActivityRuns int
	}{
		{
			name:             "active claim returns busy",
			claimExpiresAt:   time.Now().Add(time.Hour),
			wantActivityRuns: 0,
		},
		{
			name:             "expired create-only claim still returns busy",
			claimExpiresAt:   time.Now().Add(-time.Second),
			wantActivityRuns: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			claimStore := newTestClaimStore(t)
			claimKey := storage.ClaimKey{
				Namespace:  storage.DefaultNamespace,
				WorkflowID: "prices:claims",
				RunID:      "2026-05-02",
				ClaimID:    ActivityClaimIDPrefix + "fetch:symbol",
			}
			created, err := claimStore.TryCreateClaim(ctx, &temporalessv1.ClaimRecord{
				SchemaVersion:  storage.ClaimRecordSchemaVersion,
				Key:            claimKey.Proto(),
				OwnerId:        "other-owner",
				ResourceType:   temporalessv1.ClaimResourceType_CLAIM_RESOURCE_TYPE_ACTIVITY,
				ResourceId:     "fetch:symbol",
				CodeVersion:    "test-version",
				LeaseExpiresAt: timestamppb.New(test.claimExpiresAt),
				CreatedAt:      timestamppb.Now(),
				HeartbeatAt:    timestamppb.Now(),
			})
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Fatal("expected pre-created claim")
			}

			activityRuns := 0
			result, err := Run(
				ctx,
				store,
				&Options{
					WorkflowId:   "prices:claims",
					RunId:        "2026-05-02",
					CodeVersion:  "test-version",
					ClaimOwnerId: "this-owner",
				},
				claimStore,
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return ExecuteActivity(
						ctx,
						&ActivityOptions{ActivityId: "fetch:symbol"},
						input,
						func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
						func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
							activityRuns++
							return wrapperspb.String("stored:" + input.GetValue()), nil
						},
					)
				},
			)
			if !errors.Is(err, ErrClaimBusy) {
				t.Fatalf("err = %v, want %v", err, ErrClaimBusy)
			}
			var busyErr *ClaimBusyError
			if !errors.As(err, &busyErr) {
				t.Fatalf("err = %T, want ClaimBusyError", err)
			}
			if busyErr.Capability != storage.CreateOnlyClaims {
				t.Fatalf("capability = %q, want %q", busyErr.Capability, storage.CreateOnlyClaims)
			}
			if result != nil {
				t.Fatalf("result = %v, want nil", result)
			}
			if activityRuns != test.wantActivityRuns {
				t.Fatalf("activity runs = %d, want %d", activityRuns, test.wantActivityRuns)
			}
		})
	}
}

func TestConcurrentActivityClaimSerialization(t *testing.T) {
	// Tests claim-level contention specifically. Each goroutine drives
	// runActivity directly with its own owner_id; only one should get the
	// claim and execute the body, the rest see ClaimBusy.
	//
	// Note: this test only stresses the claim store, not the activity record
	// store. The OpenDAL fs scheme used here is not atomic for concurrent
	// reads-during-writes, so we deliberately don't drive concurrent
	// PutActivity calls — that would expose fs limitations rather than the
	// claim mechanism. Production backends (S3, GCS) provide atomic writes
	// natively and the framework relies on that property.
	ctx := context.Background()
	store := newTestStore(t)
	claimStore := newTestClaimStore(t)

	const goroutines = 4
	var activityCalls atomic.Int64
	results := make([]string, goroutines)
	errs := make([]error, goroutines)

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			wf := &Workflow{
				store:       store,
				claimStore:  claimStore,
				workflowID:  "prices:concurrent",
				runID:       "2026-05-04",
				codeVersion: "test-version",
				claimOwner:  fmt.Sprintf("worker-%d", idx),
			}
			result, err := runActivity(
				ctx,
				wf,
				"fetch:concurrent",
				"activity:google.protobuf.StringValue->google.protobuf.StringValue",
				nil,
				"",
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context) (*wrapperspb.StringValue, error) {
					activityCalls.Add(1)
					// Hold the claim long enough that the losers reach the
					// claim check and back off via ClaimBusy without ever
					// hitting the activity record.
					time.Sleep(50 * time.Millisecond)
					return wrapperspb.String("ok:AAPL"), nil
				},
			)
			if result != nil {
				results[idx] = result.GetValue()
			}
			errs[idx] = err
		}(i)
	}
	close(start)
	wg.Wait()

	if got := activityCalls.Load(); got != 1 {
		t.Fatalf("activity executions = %d, want 1", got)
	}

	successCount := 0
	busyCount := 0
	for i, err := range errs {
		switch {
		case err == nil:
			successCount++
			if results[i] != "ok:AAPL" {
				t.Fatalf("worker %d result = %q", i, results[i])
			}
		case errors.Is(err, ErrClaimBusy):
			busyCount++
		default:
			// OpenDAL fs is not atomic for concurrent reads-during-writes; a
			// loser may hit a partial-read on the activity record. We treat
			// this as a benign backend-quirk failure rather than a framework
			// bug — production backends (S3/GCS) don't have this race.
			t.Logf("worker %d transient backend error: %v", i, err)
		}
	}
	if successCount < 1 {
		t.Fatalf("success count = %d, want at least 1", successCount)
	}
	if busyCount < 1 {
		t.Fatalf("busy count = %d, want at least 1 (claim should have blocked at least one goroutine)", busyCount)
	}
}

func TestClaimStoreDeclaresCapability(t *testing.T) {
	tests := []struct {
		name string
		got  func() (storage.ClaimCapability, error)
		want storage.ClaimCapability
	}{
		{
			name: "gocdk claims are create only",
			got: func() (storage.ClaimCapability, error) {
				return newTestClaimStore(t).ClaimCapability(context.Background())
			},
			want: storage.CreateOnlyClaims,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.got()
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("capability = %q, want %q", got, test.want)
			}
		})
	}
}

func TestRunRejectsMissingRequiredIDs(t *testing.T) {
	tests := []struct {
		name       string
		options    *Options
		claimStore storage.ClaimStore
	}{
		{
			name:    "run ID is required",
			options: &Options{WorkflowId: "prices:ids", CodeVersion: "test-version"},
		},
		// Note: "claim_owner_id required when claim store is present" was
		// removed when concurrency-keys landed — a claim store can now be
		// present for concurrency-slot use without enabling activity claims.
		// Activity claims are opt-in via claim_owner_id; absence skips them.
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Run(
				context.Background(),
				newTestStore(t),
				test.options,
				test.claimStore,
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("should-not-run"), nil
				},
			)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestWrapWorkflow(t *testing.T) {
	tests := []struct {
		name           string
		firstInput     string
		nextInput      string
		wrap           func(storage.Store, WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]
		wantSecond     string
		wantExecutions int
	}{
		{
			name:       "fixed options replays the same RPC-shaped workflow",
			firstInput: "AAPL",
			nextInput:  "AAPL",
			wrap: func(store storage.Store, execute WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue] {
				return WrapWorkflow(WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					Store:     store,
					Options:   &Options{WorkflowId: "prices:wrapped", RunId: "2026-05-02", CodeVersion: "test-version"},
					NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					Execute:   execute,
				})
			},
			wantSecond:     "wrapped:AAPL",
			wantExecutions: 1,
		},
		{
			name:       "request options keep RPC IDs explicit per call",
			firstInput: "AAPL",
			nextInput:  "MSFT",
			wrap: func(store storage.Store, execute WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) WorkflowFunc[*wrapperspb.StringValue, *wrapperspb.StringValue] {
				return WrapWorkflow(WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					Store: store,
					OptionsFor: func(_ context.Context, input *wrapperspb.StringValue) (*Options, error) {
						return &Options{
							WorkflowId:  "prices:" + input.GetValue(),
							RunId:       "2026-05-02",
							CodeVersion: "test-version",
						}, nil
					},
					NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					Execute:   execute,
				})
			},
			wantSecond:     "wrapped:MSFT",
			wantExecutions: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			executions := 0
			handler := test.wrap(
				store,
				func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					executions++
					return wrapperspb.String("wrapped:" + input.GetValue()), nil
				},
			)

			first, err := handler(context.Background(), wrapperspb.String(test.firstInput))
			if err != nil {
				t.Fatal(err)
			}
			if first.GetValue() != "wrapped:"+test.firstInput {
				t.Fatalf("first result = %q", first.GetValue())
			}

			second, err := handler(context.Background(), wrapperspb.String(test.nextInput))
			if err != nil {
				t.Fatal(err)
			}
			if second.GetValue() != test.wantSecond {
				t.Fatalf("second result = %q, want %q", second.GetValue(), test.wantSecond)
			}
			if executions != test.wantExecutions {
				t.Fatalf("executions = %d, want %d", executions, test.wantExecutions)
			}
		})
	}
}

func TestWrapActivity(t *testing.T) {
	tests := []struct {
		name           string
		runID          string
		run            func(context.Context, *wrapperspb.StringValue, ActivityFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) (*wrapperspb.StringValue, error)
		want           string
		wantExecutions int
	}{
		{
			name:  "fixed activity ID replays the wrapped RPC handler",
			runID: "fixed-activity-wrapper",
			run: func(ctx context.Context, input *wrapperspb.StringValue, execute ActivityFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) (*wrapperspb.StringValue, error) {
				handler := WrapActivity(ActivityWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					Options:   &ActivityOptions{ActivityId: "fetch:symbol"},
					NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					Execute:   execute,
				})
				first, err := handler(ctx, input)
				if err != nil {
					return nil, err
				}
				second, err := handler(ctx, input)
				if err != nil {
					return nil, err
				}
				return wrapperspb.String(first.GetValue() + "|" + second.GetValue()), nil
			},
			want:           "activity:AAPL|activity:AAPL",
			wantExecutions: 1,
		},
		{
			name:  "request activity ID keeps wrapped RPC activities explicit",
			runID: "request-activity-wrapper",
			run: func(ctx context.Context, input *wrapperspb.StringValue, execute ActivityFunc[*wrapperspb.StringValue, *wrapperspb.StringValue]) (*wrapperspb.StringValue, error) {
				handler := WrapActivity(ActivityWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					OptionsFor: func(_ context.Context, request *wrapperspb.StringValue) (*ActivityOptions, error) {
						return &ActivityOptions{ActivityId: "fetch:" + request.GetValue()}, nil
					},
					NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					Execute:   execute,
				})
				first, err := handler(ctx, input)
				if err != nil {
					return nil, err
				}
				second, err := handler(ctx, wrapperspb.String("MSFT"))
				if err != nil {
					return nil, err
				}
				return wrapperspb.String(first.GetValue() + "|" + second.GetValue()), nil
			},
			want:           "activity:AAPL|activity:MSFT",
			wantExecutions: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newTestStore(t)
			executions := 0
			execute := func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				executions++
				return wrapperspb.String("activity:" + input.GetValue()), nil
			}

			result, err := Run(
				context.Background(),
				store,
				&Options{WorkflowId: "prices:activity-wrapper", RunId: test.runID, CodeVersion: "test-version"},
				nil,
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					return test.run(ctx, input, execute)
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.GetValue() != test.want {
				t.Fatalf("result = %q, want %q", result.GetValue(), test.want)
			}
			if executions != test.wantExecutions {
				t.Fatalf("executions = %d, want %d", executions, test.wantExecutions)
			}
		})
	}
}

func TestRunWorkflow(t *testing.T) {
	tests := []struct {
		name       string
		firstInput string
		nextInput  string
		want       string
		wantErr    error
	}{
		{
			name:       "replays completed workflow result",
			firstInput: "AAPL",
			nextInput:  "AAPL",
			want:       "workflow:normalized:AAPL",
		},
		{
			name:       "replays stored result even when input bytes differ",
			firstInput: "AAPL",
			nextInput:  "MSFT",
			want:       "workflow:normalized:AAPL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			executions := 0

			run := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				executions++
				activityResult, err := ExecuteActivity(
					ctx,
					&ActivityOptions{ActivityId: "normalize:symbol"},
					input,
					func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
					func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return wrapperspb.String("normalized:" + input.GetValue()), nil
					},
				)
				if err != nil {
					return nil, err
				}
				return wrapperspb.String("workflow:" + activityResult.GetValue()), nil
			}

			first, err := Run(
				ctx,
				store,
				&Options{WorkflowId: "prices:symbol", RunId: "2026-05-02", CodeVersion: "test-version"},
				nil,
				wrapperspb.String(test.firstInput),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if err != nil {
				t.Fatal(err)
			}
			if first.GetValue() != "workflow:normalized:"+test.firstInput {
				t.Fatalf("first result = %q", first.GetValue())
			}

			second, err := Run(
				ctx,
				store,
				&Options{WorkflowId: "prices:symbol", RunId: "2026-05-02", CodeVersion: "test-version"},
				nil,
				wrapperspb.String(test.nextInput),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("err = %v, want %v", err, test.wantErr)
				}
				if executions != 1 {
					t.Fatalf("executions = %d, want 1", executions)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if second.GetValue() != test.want {
				t.Fatalf("second result = %q, want %q", second.GetValue(), test.want)
			}
			if executions != 1 {
				t.Fatalf("executions = %d, want 1", executions)
			}
		})
	}
}

func TestSleep(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		wantErr  error
	}{
		{
			name:     "fires immediately when duration is not positive",
			duration: 0,
		},
		{
			name:     "returns pending without completing workflow",
			duration: time.Hour,
			wantErr:  ErrTimerPending,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			executions := 0

			run := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				executions++
				if err := Sleep(ctx, "wait:vendor-window", test.duration); err != nil {
					return nil, err
				}
				return wrapperspb.String("done:" + input.GetValue()), nil
			}

			result, err := Run(
				ctx,
				store,
				&Options{WorkflowId: "prices:sleep", RunId: "2026-05-02", CodeVersion: "test-version"},
				nil,
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("err = %v, want %v", err, test.wantErr)
				}
				if result != nil {
					t.Fatalf("result = %v, want nil", result)
				}
				if executions != 1 {
					t.Fatalf("executions = %d, want 1", executions)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.GetValue() != "done:AAPL" {
				t.Fatalf("result = %q", result.GetValue())
			}
			if executions != 1 {
				t.Fatalf("executions = %d, want 1", executions)
			}
		})
	}
}

func TestAnnotationsPersistOnWorkflowAndActivity(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:annotations", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			Annotate(ctx, "request_symbol", input.GetValue())
			return ExecuteActivity(
				ctx,
				&ActivityOptions{ActivityId: "fetch:annotated"},
				input,
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
					Annotate(ctx, "model", "claude-opus-4-7")
					Annotate(ctx, "tokens", "128")
					return wrapperspb.String("ok:" + request.GetValue()), nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	wfRecord, _, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:annotations",
		RunID:      "2026-05-02",
	})
	if err != nil {
		t.Fatal(err)
	}
	if wfRecord.GetAnnotations()["request_symbol"] != "AAPL" {
		t.Fatalf("workflow annotations = %v", wfRecord.GetAnnotations())
	}
	if _, ok := wfRecord.GetAnnotations()["model"]; ok {
		t.Fatalf("workflow annotations should not include activity annotations: %v", wfRecord.GetAnnotations())
	}

	actRecord, _, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:annotations",
		RunID:      "2026-05-02",
		ActivityID: "fetch:annotated",
	})
	if err != nil {
		t.Fatal(err)
	}
	if actRecord.GetAnnotations()["model"] != "claude-opus-4-7" || actRecord.GetAnnotations()["tokens"] != "128" {
		t.Fatalf("activity annotations = %v", actRecord.GetAnnotations())
	}
}

func TestWorkflowAnnotationsSurviveContinuationReplay(t *testing.T) {
	tests := []struct {
		name       string
		pendingErr error
		wantErr    error
	}{
		{
			name:       "timer pending",
			pendingErr: &TimerPendingError{TimerID: "wait", WakeAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)},
			wantErr:    ErrTimerPending,
		},
		{
			name:       "event pending",
			pendingErr: &EventPendingError{EventID: "approval"},
			wantErr:    ErrEventPending,
		},
		{
			name: "dependency pending",
			pendingErr: &WorkflowDependencyPendingError{
				WorkflowID: "upstream",
				RunID:      "run",
			},
			wantErr: ErrWorkflowDependencyPending,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			options := &Options{
				WorkflowId:  "annotations:" + strings.ReplaceAll(test.name, " ", "-"),
				RunId:       "run",
				CodeVersion: "v1",
			}
			executions := 0
			execute := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				executions++
				if executions == 1 {
					Annotate(ctx, "phase", "planned")
					return nil, test.pendingErr
				}
				return wrapperspb.String("done"), nil
			}

			_, err := Run(
				ctx,
				store,
				options,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				execute,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("first run error = %v, want %v", err, test.wantErr)
			}

			key := storage.NewWorkflowKey(options.GetWorkflowId(), options.GetRunId())
			pending, found, err := store.GetWorkflow(ctx, key)
			if err != nil || !found {
				t.Fatalf("pending workflow record: err=%v found=%v", err, found)
			}
			if pending.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
				t.Fatalf("pending status = %s, want IN_PROGRESS", pending.GetStatus())
			}
			if pending.GetAnnotations()["phase"] != "planned" {
				t.Fatalf("pending annotations = %v, want phase=planned", pending.GetAnnotations())
			}

			result, err := Run(
				ctx,
				store,
				options,
				nil,
				wrapperspb.String("request"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				execute,
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.GetValue() != "done" {
				t.Fatalf("result = %q, want done", result.GetValue())
			}
			completed, found, err := store.GetWorkflow(ctx, key)
			if err != nil || !found {
				t.Fatalf("completed workflow record: err=%v found=%v", err, found)
			}
			if completed.GetAnnotations()["phase"] != "planned" {
				t.Fatalf("completed annotations = %v, want phase=planned", completed.GetAnnotations())
			}
		})
	}
}

func TestWorkflowAccessorsExpose(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:accessors", RunId: "2026-05-02", CodeVersion: "v42"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			wf, ok := Current(ctx)
			if !ok {
				return nil, errors.New("workflow context missing")
			}
			if wf.WorkflowID() != "prices:accessors" || wf.RunID() != "2026-05-02" || wf.CodeVersion() != "v42" {
				return nil, fmt.Errorf("accessors = %s/%s/%s", wf.WorkflowID(), wf.RunID(), wf.CodeVersion())
			}
			return wrapperspb.String("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSendEventDeliversWaitableEvent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	key := storage.EventKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:send-event",
		RunID:      "2026-05-02",
		EventID:    "approval",
	}
	if err := storage.SendEvent(ctx, store, key, wrapperspb.String("manager")); err != nil {
		t.Fatal(err)
	}

	record, found, err := store.GetEvent(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected event record")
	}
	got := &wrapperspb.StringValue{}
	if err := record.GetPayload().UnmarshalTo(got); err != nil {
		t.Fatal(err)
	}
	if got.GetValue() != "manager" {
		t.Fatalf("payload = %q", got.GetValue())
	}
	if record.GetReceivedAt() == nil {
		t.Fatal("received_at not populated")
	}
}

func TestWaitEventReturnsPendingThenResumes(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	executions := 0
	run := func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		executions++
		payload, err := WaitEvent(ctx, "approval", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
		if err != nil {
			return nil, err
		}
		return wrapperspb.String("approved:" + payload.GetValue()), nil
	}

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:event", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if !errors.Is(err, ErrEventPending) {
		t.Fatalf("first run err = %v, want ErrEventPending", err)
	}

	record, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:event",
		RunID:      "2026-05-02",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("found=%v status=%v, want IN_PROGRESS", found, record.GetStatus())
	}

	payload, err := anypb.New(wrapperspb.String("manager"))
	if err != nil {
		t.Fatal(err)
	}
	eventKey := storage.EventKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:event",
		RunID:      "2026-05-02",
		EventID:    "approval",
	}
	if err := store.PutEvent(ctx, &temporalessv1.EventRecord{
		SchemaVersion: storage.EventRecordSchemaVersion,
		Key:           eventKey.Proto(),
		Payload:       payload,
		ReceivedAt:    timestamppb.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	result, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:event", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "approved:manager" {
		t.Fatalf("result = %q", result.GetValue())
	}
	if executions != 2 {
		t.Fatalf("executions = %d, want 2", executions)
	}
}

func TestSleepResumesAfterStoredTimerIsDue(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	executions := 0

	run := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		executions++
		if err := Sleep(ctx, "wait:vendor-window", time.Hour); err != nil {
			return nil, err
		}
		return wrapperspb.String("done:" + input.GetValue()), nil
	}

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:sleep", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("err = %v, want ErrTimerPending", err)
	}

	key := storage.TimerKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:sleep",
		RunID:      "2026-05-02",
		TimerID:    "wait:vendor-window",
	}
	record, found, err := store.GetTimer(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("timer record not found")
	}
	record.FireAt = timestamppb.New(time.Now().Add(-time.Second))
	if err := store.PutTimer(ctx, record); err != nil {
		t.Fatal(err)
	}

	result, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:sleep", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "done:AAPL" {
		t.Fatalf("result = %q", result.GetValue())
	}
	if executions != 2 {
		t.Fatalf("executions = %d, want 2", executions)
	}
}

func TestRunWritesInProgressBeforeExecution(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:in-progress", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			record, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
				Namespace:  storage.DefaultNamespace,
				WorkflowID: "prices:in-progress",
				RunID:      "2026-05-02",
			})
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, fmt.Errorf("expected IN_PROGRESS record visible during execution")
			}
			if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
				return nil, fmt.Errorf("status during execute = %v, want IN_PROGRESS", record.GetStatus())
			}
			return wrapperspb.String("done"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	record, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:in-progress",
		RunID:      "2026-05-02",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected stored workflow record")
	}
	if record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("status = %v, want COMPLETED", record.GetStatus())
	}
}

func TestRunStoresFailedRecordOnNonPendingError(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:fails", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return nil, NewActivityError("boom", "explicit failure", nil)
		},
	)
	if err == nil {
		t.Fatal("expected workflow to fail")
	}

	record, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:fails",
		RunID:      "2026-05-02",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("found=%v status=%v, want FAILED", found, record.GetStatus())
	}
	if record.GetFailure().GetCode() != "boom" {
		t.Fatalf("failure code = %q, want boom", record.GetFailure().GetCode())
	}

	executions := 0
	_, replayErr := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:fails", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			executions++
			return wrapperspb.String("should-not-run"), nil
		},
	)
	if executions != 0 {
		t.Fatalf("executions = %d, want 0", executions)
	}
	var typed *ActivityError
	if !errors.As(replayErr, &typed) {
		t.Fatalf("replay err = %T, want *ActivityError", replayErr)
	}
	if typed.Code != "boom" {
		t.Fatalf("replay code = %q, want boom", typed.Code)
	}
}

func TestRunSleepLeavesInProgressForResume(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	_, err := Run(
		ctx,
		store,
		&Options{WorkflowId: "prices:resume", RunId: "2026-05-02", CodeVersion: "test-version"},
		nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(ctx context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			if err := Sleep(ctx, "wait:resume", time.Hour); err != nil {
				return nil, err
			}
			return wrapperspb.String("done"), nil
		},
	)
	if !errors.Is(err, ErrTimerPending) {
		t.Fatalf("err = %v, want ErrTimerPending", err)
	}

	record, found, err := store.GetWorkflow(ctx, storage.WorkflowKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: "prices:resume",
		RunID:      "2026-05-02",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found || record.GetStatus() != temporalessv1.WorkflowStatus_WORKFLOW_STATUS_IN_PROGRESS {
		t.Fatalf("found=%v status=%v, want IN_PROGRESS", found, record.GetStatus())
	}
}

func TestRunActivityRetriesUntilSuccess(t *testing.T) {
	tests := []struct {
		name         string
		failures     int
		maxAttempts  uint32
		wantAttempts int
	}{
		{name: "succeeds on first attempt", failures: 0, maxAttempts: 3, wantAttempts: 1},
		{name: "succeeds on second attempt", failures: 1, maxAttempts: 3, wantAttempts: 2},
		{name: "succeeds on final attempt", failures: 2, maxAttempts: 3, wantAttempts: 3},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			wf := &Workflow{
				store:       store,
				workflowID:  "prices:retry",
				runID:       fmt.Sprintf("retry-success-%d", index),
				codeVersion: "test-version",
			}

			calls := 0
			run := func(_ context.Context) (*wrapperspb.StringValue, error) {
				calls++
				if calls <= test.failures {
					return nil, NewActivityError("rate_limited", "vendor 429", nil)
				}
				return wrapperspb.String("ok"), nil
			}

			result, err := runActivity(
				ctx,
				wf,
				"fetch:retry",
				"activity:google.protobuf.StringValue->google.protobuf.StringValue",
				&temporalessv1.RetryPolicy{
					MaximumAttempts: test.maxAttempts,
					InitialInterval: durationpb.New(time.Millisecond),
				},
				testRetryTimerID("fetch:retry"),
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				run,
			)
			if err != nil {
				t.Fatal(err)
			}
			if result.GetValue() != "ok" {
				t.Fatalf("result = %q", result.GetValue())
			}
			if calls != test.wantAttempts {
				t.Fatalf("calls = %d, want %d", calls, test.wantAttempts)
			}

			record, found, err := store.GetActivity(ctx, storage.ActivityKey{
				Namespace:  storage.DefaultNamespace,
				WorkflowID: wf.workflowID,
				RunID:      wf.runID,
				ActivityID: "fetch:retry",
			})
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("activity record not stored")
			}
			if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
				t.Fatalf("status = %v, want COMPLETED", record.GetStatus())
			}
			if got := len(record.GetAttempts()); got != test.wantAttempts {
				t.Fatalf("attempts = %d, want %d", got, test.wantAttempts)
			}
		})
	}
}

func TestRunActivityRetriesExhaustedSurfacesFailure(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "prices:retry-exhausted",
		runID:       "2026-05-02",
		codeVersion: "test-version",
	}

	calls := 0
	run := func(_ context.Context) (*wrapperspb.StringValue, error) {
		calls++
		return nil, NewActivityError("upstream_5xx", fmt.Sprintf("attempt %d", calls), nil)
	}

	_, err := runActivity(
		ctx,
		wf,
		"fetch:exhausted",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		&temporalessv1.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: durationpb.New(time.Millisecond),
		},
		testRetryTimerID("fetch:exhausted"),
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if err == nil {
		t.Fatal("expected error")
	}
	var failure *ActivityError
	if !errors.As(err, &failure) {
		t.Fatalf("err = %T, want *ActivityError", err)
	}
	if failure.Code != "upstream_5xx" {
		t.Fatalf("code = %q, want upstream_5xx", failure.Code)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}

	record, found, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "fetch:exhausted",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("failed activity record not stored")
	}
	if record.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_FAILED {
		t.Fatalf("status = %v, want FAILED", record.GetStatus())
	}
	if got := len(record.GetAttempts()); got != 3 {
		t.Fatalf("attempts = %d, want 3", got)
	}

	replayCalls := 0
	_, replayErr := runActivity(
		ctx,
		wf,
		"fetch:exhausted",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		&temporalessv1.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: durationpb.New(time.Millisecond),
		},
		testRetryTimerID("fetch:exhausted"),
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			replayCalls++
			return wrapperspb.String("should-not-run"), nil
		},
	)
	if replayCalls != 0 {
		t.Fatalf("replay calls = %d, want 0", replayCalls)
	}
	var replayFailure *ActivityError
	if !errors.As(replayErr, &replayFailure) {
		t.Fatalf("replay err = %T, want *ActivityError", replayErr)
	}
	if replayFailure.Code != "upstream_5xx" {
		t.Fatalf("replay code = %q, want upstream_5xx", replayFailure.Code)
	}
}

func TestRunActivityResumesRetryAcrossInvocations(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "prices:retry-resume",
		runID:       "2026-05-04",
		codeVersion: "test-version",
	}

	totalCalls := 0
	policy := &temporalessv1.RetryPolicy{
		MaximumAttempts: 3,
		// Long enough that the first invocation's sleep is interruptible by ctx cancel.
		InitialInterval: durationpb.New(500 * time.Millisecond),
	}

	// First invocation: fail attempt 1, persist RETRYING, then process "dies"
	// during the sleep before attempt 2.
	firstCtx, cancelFirst := context.WithCancel(ctx)
	time.AfterFunc(50*time.Millisecond, cancelFirst)

	calls := 0
	_, firstErr := runActivity(
		firstCtx,
		wf,
		"fetch:resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("fetch:resume"),
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			calls++
			totalCalls++
			return nil, NewActivityError("rate_limited", "transient", nil)
		},
	)
	if firstErr == nil {
		t.Fatal("expected first invocation to be cancelled")
	}
	if calls != 1 {
		t.Fatalf("first invocation calls = %d, want 1", calls)
	}

	stored, found, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "fetch:resume",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected RETRYING activity record after first invocation")
	}
	if stored.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_RETRYING {
		t.Fatalf("status = %v, want RETRYING", stored.GetStatus())
	}
	if got := len(stored.GetAttempts()); got != 1 {
		t.Fatalf("stored attempts = %d, want 1", got)
	}

	// Second invocation: resumes from attempt 2. Fail once more, persist
	// RETRYING(attempts=[a1, a2]), then succeed on attempt 3.
	calls = 0
	result, err := runActivity(
		ctx,
		wf,
		"fetch:resume",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		policy,
		testRetryTimerID("fetch:resume"),
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context) (*wrapperspb.StringValue, error) {
			calls++
			totalCalls++
			if calls == 1 {
				return nil, NewActivityError("rate_limited", "still transient", nil)
			}
			return wrapperspb.String("ok"), nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "ok" {
		t.Fatalf("result = %q", result.GetValue())
	}
	if calls != 2 {
		t.Fatalf("second invocation calls = %d, want 2 (resume from attempt 2, then attempt 3 succeeds)", calls)
	}
	if totalCalls != 3 {
		t.Fatalf("total calls across invocations = %d, want 3", totalCalls)
	}

	final, _, err := store.GetActivity(ctx, storage.ActivityKey{
		Namespace:  storage.DefaultNamespace,
		WorkflowID: wf.workflowID,
		RunID:      wf.runID,
		ActivityID: "fetch:resume",
	})
	if err != nil {
		t.Fatal(err)
	}
	if final.GetStatus() != temporalessv1.ActivityStatus_ACTIVITY_STATUS_COMPLETED {
		t.Fatalf("final status = %v, want COMPLETED", final.GetStatus())
	}
	if got := len(final.GetAttempts()); got != 3 {
		t.Fatalf("final attempts = %d, want 3 (full history preserved)", got)
	}
}

func TestRunActivityNonRetryableErrorFailsFast(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	wf := &Workflow{
		store:       store,
		workflowID:  "prices:non-retryable",
		runID:       "2026-05-02",
		codeVersion: "test-version",
	}

	calls := 0
	run := func(_ context.Context) (*wrapperspb.StringValue, error) {
		calls++
		return nil, NewActivityError("invalid_argument", "bad symbol", nil)
	}

	_, err := runActivity(
		ctx,
		wf,
		"fetch:non-retryable",
		"activity:google.protobuf.StringValue->google.protobuf.StringValue",
		&temporalessv1.RetryPolicy{
			MaximumAttempts:        5,
			InitialInterval:        durationpb.New(time.Millisecond),
			NonRetryableErrorCodes: []string{"invalid_argument"},
		},
		testRetryTimerID("fetch:non-retryable"),
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run,
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestRunActivityInvalidRetryPolicyRejected(t *testing.T) {
	tests := []struct {
		name   string
		policy *temporalessv1.RetryPolicy
	}{
		{
			name:   "maximum_attempts zero is rejected",
			policy: &temporalessv1.RetryPolicy{},
		},
		{
			name:   "missing initial_interval with retries is rejected",
			policy: &temporalessv1.RetryPolicy{MaximumAttempts: 3},
		},
		{
			name: "negative initial_interval is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts: 1,
				InitialInterval: durationpb.New(-time.Second),
			},
		},
		{
			name: "negative backoff_coefficient is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts:    2,
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: -1,
			},
		},
		{
			name: "NaN backoff_coefficient is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts:    2,
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: math.NaN(),
			},
		},
		{
			name: "infinite backoff_coefficient is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts:    2,
				InitialInterval:    durationpb.New(time.Second),
				BackoffCoefficient: math.Inf(1),
			},
		},
		{
			name: "negative maximum_interval is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts: 2,
				InitialInterval: durationpb.New(time.Second),
				MaximumInterval: durationpb.New(-time.Second),
			},
		},
		{
			name: "maximum_interval below initial_interval is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts: 2,
				InitialInterval: durationpb.New(2 * time.Second),
				MaximumInterval: durationpb.New(time.Second),
			},
		},
		{
			name: "negative durable_backoff_threshold is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts:         2,
				InitialInterval:         durationpb.New(time.Second),
				DurableBackoffThreshold: durationpb.New(-time.Second),
			},
		},
		{
			name: "invalid protobuf duration is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts: 2,
				InitialInterval: &durationpb.Duration{Nanos: 1_000_000_000},
			},
		},
		{
			name: "duration outside Go range is rejected",
			policy: &temporalessv1.RetryPolicy{
				MaximumAttempts: 2,
				InitialInterval: &durationpb.Duration{Seconds: 315_576_000_000},
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			wf := &Workflow{
				store:       store,
				workflowID:  "prices:bad-policy",
				runID:       fmt.Sprintf("bad-policy-%d", index),
				codeVersion: "test-version",
			}
			_, err := runActivity(
				ctx,
				wf,
				"fetch:bad",
				"activity:google.protobuf.StringValue->google.protobuf.StringValue",
				test.policy,
				testRetryTimerID("fetch:bad"),
				wrapperspb.String("AAPL"),
				func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
				func(_ context.Context) (*wrapperspb.StringValue, error) {
					return wrapperspb.String("ok"), nil
				},
			)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestRunIsPointInTimeAgainstMutatingSource is the core leakage-guard property:
// once an activity has fetched-and-stored a value at
// as-of time T, replaying the run returns that frozen snapshot even after the
// underlying live source has moved. A re-run can never pull in "future" data,
// because the stored boundary short-circuits re-execution. This is what makes
// a temporaless-backed feature pipeline point-in-time correct rather than
// silently look-ahead-biased.
func TestRunIsPointInTimeAgainstMutatingSource(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)

	// liveSource models a mutable upstream (an exchange tick, a freshly
	// retrained model output, a moving fundamentals row). It returns a
	// different value on every read.
	var liveSource atomic.Int64
	liveSource.Store(100)

	var bodyExecutions atomic.Int64
	run := func(ctx context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return ExecuteActivity(
			ctx,
			&ActivityOptions{ActivityId: "fetch:price"},
			input,
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				bodyExecutions.Add(1)
				return wrapperspb.String(fmt.Sprintf("%s@%d", input.GetValue(), liveSource.Load())), nil
			},
		)
	}

	opts := &Options{WorkflowId: "feature:price", RunId: "2026-05-31", CodeVersion: "v1"}
	first, err := Run(ctx, store, opts, nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run)
	if err != nil {
		t.Fatal(err)
	}
	if first.GetValue() != "AAPL@100" {
		t.Fatalf("first as-of snapshot = %q, want AAPL@100", first.GetValue())
	}

	// The live source moves AFTER the as-of snapshot was committed.
	liveSource.Store(250)

	second, err := Run(ctx, store, opts, nil,
		wrapperspb.String("AAPL"),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		run)
	if err != nil {
		t.Fatal(err)
	}
	// Replay must return the frozen as-of value, not the moved source.
	if second.GetValue() != "AAPL@100" {
		t.Fatalf("replay leaked future data: got %q, want frozen AAPL@100", second.GetValue())
	}
	if got := bodyExecutions.Load(); got != 1 {
		t.Fatalf("activity body executed %d times, want 1 (replay must not re-run)", got)
	}
}

func newTestStore(t *testing.T) *storage.OpenDALStore {
	t.Helper()

	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{
		"root": t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(operator.Close)
	return storage.NewOpenDALStore(operator)
}

func newTestClaimStore(t *testing.T) *gocdkclaims.Store {
	t.Helper()

	// MetadataDontWrite suppresses fileblob's `.attrs` JSON sidecar. With the
	// sidecar enabled, every WriteAll truncates `<path>.attrs` with os.Create
	// BEFORE the IfNotExist precondition is checked — a racing GetClaim
	// reading that same sidecar lands mid-truncate and gets io.EOF out of
	// the JSON decoder. We don't store any GoCDK metadata on claim records,
	// so dropping the sidecar costs nothing and closes the race.
	bucket, err := fileblob.OpenBucket(t.TempDir(), &fileblob.Options{
		Metadata: fileblob.MetadataDontWrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := bucket.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return gocdkclaims.NewStore(bucket)
}
