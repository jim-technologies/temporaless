package workflow

import (
	"context"
	"fmt"

	"github.com/jim-technologies/temporaless/core/go/storage"
	"google.golang.org/protobuf/proto"
)

type WorkflowWrapOptions[Req proto.Message, Resp proto.Message] struct {
	Store      storage.Store
	ClaimStore storage.ClaimStore
	Options    *Options
	OptionsFor func(context.Context, Req) (*Options, error)
	NewResult  func() Resp
	Execute    WorkflowFunc[Req, Resp]
}

type ActivityWrapOptions[Req proto.Message, Resp proto.Message] struct {
	Options    *ActivityOptions
	OptionsFor func(context.Context, Req) (*ActivityOptions, error)
	NewResult  func() Resp
	Execute    ActivityFunc[Req, Resp]
}

func WrapWorkflow[Req proto.Message, Resp proto.Message](
	options WorkflowWrapOptions[Req, Resp],
) WorkflowFunc[Req, Resp] {
	return func(ctx context.Context, input Req) (Resp, error) {
		if options.Options != nil && options.OptionsFor != nil {
			var zero Resp
			return zero, fmt.Errorf("workflow wrap options are ambiguous: set Options OR OptionsFor, not both")
		}
		var runOptions *Options
		if options.OptionsFor != nil {
			resolved, err := options.OptionsFor(ctx, input)
			if err != nil {
				var zero Resp
				return zero, err
			}
			runOptions = resolved
		} else if options.Options != nil {
			runOptions = options.Options
		} else {
			var zero Resp
			return zero, fmt.Errorf("workflow run options are required")
		}
		return Run(ctx, options.Store, runOptions, options.ClaimStore, input, options.NewResult, options.Execute)
	}
}

func WrapActivity[Req proto.Message, Resp proto.Message](
	options ActivityWrapOptions[Req, Resp],
) ActivityFunc[Req, Resp] {
	return func(ctx context.Context, input Req) (Resp, error) {
		if options.Options != nil && options.OptionsFor != nil {
			var zero Resp
			return zero, fmt.Errorf("activity wrap options are ambiguous: set Options OR OptionsFor, not both")
		}
		var activityOptions *ActivityOptions
		if options.OptionsFor != nil {
			resolved, err := options.OptionsFor(ctx, input)
			if err != nil {
				var zero Resp
				return zero, err
			}
			activityOptions = resolved
		} else if options.Options != nil {
			activityOptions = options.Options
		} else {
			var zero Resp
			return zero, fmt.Errorf("activity options are required")
		}
		if activityOptions == nil {
			var zero Resp
			return zero, fmt.Errorf("activity options are required")
		}
		return ExecuteActivity(ctx, activityOptions, input, options.NewResult, options.Execute)
	}
}
