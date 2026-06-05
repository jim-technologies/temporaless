package temporalcompat

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var retryActivityAttempts int

func TestExecuteActivityUsesTemporalSDK(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(priceWorkflow)
	env.RegisterActivity(fetchPriceActivity)

	env.ExecuteWorkflow(priceWorkflow, wrapperspb.String("AAPL"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result wrapperspb.StringValue
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "AAPL 100.00" {
		t.Fatalf("result = %q, want %q", result.GetValue(), "AAPL 100.00")
	}
}

func TestSleepUsesTemporalSDKTimer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(sleepWorkflow)

	start := env.Now()
	env.ExecuteWorkflow(sleepWorkflow, wrapperspb.String("AAPL"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if elapsed := env.Now().Sub(start); elapsed < time.Hour {
		t.Fatalf("temporal test clock elapsed = %s, want at least 1h", elapsed)
	}
}

func TestRetryPolicyUsesTemporalSDK(t *testing.T) {
	retryActivityAttempts = 0
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(retryWorkflow)
	env.RegisterActivity(flakyPriceActivity)

	env.ExecuteWorkflow(retryWorkflow, wrapperspb.String("AAPL"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result wrapperspb.StringValue
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "attempts:3" {
		t.Fatalf("result = %q, want %q", result.GetValue(), "attempts:3")
	}
	if retryActivityAttempts != 3 {
		t.Fatalf("attempts = %d, want 3", retryActivityAttempts)
	}
}

func TestTimeoutUsesTemporalSDK(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(timeoutWorkflow)
	env.RegisterActivity(fetchPriceActivity)
	observedOptions := false
	env.SetOnActivityStartedListener(func(activityInfo *activity.Info, _ context.Context, _ converter.EncodedValues) {
		observedOptions = true
		if activityInfo.ScheduleToCloseTimeout != time.Minute {
			t.Fatalf("schedule-to-close timeout = %s, want %s", activityInfo.ScheduleToCloseTimeout, time.Minute)
		}
		if activityInfo.StartToCloseTimeout != time.Minute {
			t.Fatalf("start-to-close timeout = %s, want %s", activityInfo.StartToCloseTimeout, time.Minute)
		}
	})
	env.OnActivity(fetchPriceActivity, mock.Anything, mock.Anything).
		Return((*wrapperspb.StringValue)(nil), temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil)).
		Once()

	env.ExecuteWorkflow(timeoutWorkflow, wrapperspb.String("AAPL"))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	var result wrapperspb.StringValue
	if err := env.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.GetValue() != "timeout" {
		t.Fatalf("result = %q, want %q", result.GetValue(), "timeout")
	}
	if !observedOptions {
		t.Fatal("activity options were not observed")
	}
	env.AssertExpectations(t)
}

func TestWrappersRejectNonTemporalessShape(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{
			name: "workflow nil input",
			run: func() error {
				handler := WrapWorkflow(WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					Execute: func(_ workflow.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return input, nil
					},
				})
				_, err := handler(nil, nil)
				return err
			},
			want: "workflow input is required",
		},
		{
			name: "activity nil result",
			run: func() error {
				handler := WrapActivity(ActivityWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
					Execute: func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
						return nil, nil
					},
				})
				_, err := handler(context.Background(), wrapperspb.String("AAPL"))
				return err
			},
			want: "activity returned a nil result",
		},
		{
			name: "activity call missing result constructor",
			run: func() error {
				_, err := ExecuteActivity(
					nil,
					ActivityCall[*wrapperspb.StringValue, *wrapperspb.StringValue]{
						Activity: fetchPriceActivity,
					},
					wrapperspb.String("AAPL"),
				)
				return err
			},
			want: "result constructor is required",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.run()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err = %q, want it to contain %q", err.Error(), test.want)
			}
		})
	}
}

func priceWorkflow(ctx workflow.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return ExecuteActivity(
		ctx,
		ActivityCall[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Activity: fetchPriceActivity,
			Options: workflow.ActivityOptions{
				StartToCloseTimeout: time.Minute,
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		},
		input,
	)
}

func sleepWorkflow(ctx workflow.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if err := Sleep(ctx, time.Hour); err != nil {
		return nil, err
	}
	return wrapperspb.String("done:" + input.GetValue()), nil
}

func retryWorkflow(ctx workflow.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return ExecuteActivity(
		ctx,
		ActivityCall[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Activity: flakyPriceActivity,
			Options: workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    time.Millisecond,
					BackoffCoefficient: 1,
					MaximumAttempts:    3,
				},
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		},
		input,
	)
}

func timeoutWorkflow(ctx workflow.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	_, err := ExecuteActivity(
		ctx,
		ActivityCall[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Activity: fetchPriceActivity,
			Options: workflow.ActivityOptions{
				ScheduleToCloseTimeout: time.Minute,
				StartToCloseTimeout:    time.Minute,
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: 1,
				},
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		},
		input,
	)
	if err != nil {
		var timeoutErr *temporal.TimeoutError
		if errors.As(err, &timeoutErr) {
			return wrapperspb.String("timeout"), nil
		}
		return nil, err
	}
	return wrapperspb.String("unexpected"), nil
}

func fetchPriceActivity(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	return wrapperspb.String(input.GetValue() + " 100.00"), nil
}

func flakyPriceActivity(_ context.Context, _ *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	retryActivityAttempts++
	if retryActivityAttempts < 3 {
		return nil, temporal.NewApplicationError("vendor unavailable", "VendorUnavailable")
	}
	return wrapperspb.String("attempts:3"), nil
}
