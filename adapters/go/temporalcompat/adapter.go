package temporalcompat

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/proto"
)

type WorkflowFunc[Req proto.Message, Resp proto.Message] func(workflow.Context, Req) (Resp, error)
type ActivityFunc[Req proto.Message, Resp proto.Message] func(context.Context, Req) (Resp, error)

type WorkflowWrapOptions[Req proto.Message, Resp proto.Message] struct {
	Execute WorkflowFunc[Req, Resp]
}

type ActivityWrapOptions[Req proto.Message, Resp proto.Message] struct {
	Execute ActivityFunc[Req, Resp]
}

type ActivityCall[Req proto.Message, Resp proto.Message] struct {
	Activity  ActivityFunc[Req, Resp]
	Options   workflow.ActivityOptions
	NewResult func() Resp
}

func WrapWorkflow[Req proto.Message, Resp proto.Message](
	options WorkflowWrapOptions[Req, Resp],
) WorkflowFunc[Req, Resp] {
	return func(ctx workflow.Context, input Req) (Resp, error) {
		var zero Resp
		if options.Execute == nil {
			return zero, fmt.Errorf("temporal workflow executor is required")
		}
		if isNilMessage(input) {
			return zero, fmt.Errorf("temporal workflow input is required")
		}
		result, err := options.Execute(ctx, input)
		if err != nil {
			return zero, err
		}
		if isNilMessage(result) {
			return zero, fmt.Errorf("temporal workflow returned a nil result")
		}
		return result, nil
	}
}

func WrapActivity[Req proto.Message, Resp proto.Message](
	options ActivityWrapOptions[Req, Resp],
) ActivityFunc[Req, Resp] {
	return func(ctx context.Context, input Req) (Resp, error) {
		var zero Resp
		if options.Execute == nil {
			return zero, fmt.Errorf("temporal activity executor is required")
		}
		if isNilMessage(input) {
			return zero, fmt.Errorf("temporal activity input is required")
		}
		result, err := options.Execute(ctx, input)
		if err != nil {
			return zero, err
		}
		if isNilMessage(result) {
			return zero, fmt.Errorf("temporal activity returned a nil result")
		}
		return result, nil
	}
}

func ExecuteActivity[Req proto.Message, Resp proto.Message](
	ctx workflow.Context,
	call ActivityCall[Req, Resp],
	input Req,
) (Resp, error) {
	var zero Resp
	if call.Activity == nil {
		return zero, fmt.Errorf("temporal activity is required")
	}
	if isNilMessage(input) {
		return zero, fmt.Errorf("temporal activity input is required")
	}
	if call.NewResult == nil {
		return zero, fmt.Errorf("temporal activity result constructor is required")
	}
	result := call.NewResult()
	if isNilMessage(result) {
		return zero, fmt.Errorf("temporal activity result constructor returned nil")
	}

	ctx = workflow.WithActivityOptions(ctx, call.Options)
	if err := workflow.ExecuteActivity(ctx, call.Activity, input).Get(ctx, result); err != nil {
		return zero, err
	}
	return result, nil
}

func Sleep(ctx workflow.Context, duration time.Duration) error {
	return workflow.Sleep(ctx, duration)
}

func isNilMessage(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
