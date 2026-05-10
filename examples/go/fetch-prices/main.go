package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
	root, err := os.MkdirTemp("", "temporaless-")
	if err != nil {
		panic(err)
	}
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		panic(err)
	}
	defer operator.Close()

	store := storage.NewOpenDALStore(operator)
	handler := workflow.WrapWorkflow(workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
		Store:     store,
		Options:   &workflow.Options{WorkflowId: "prices:aapl", RunId: "2026-05-02", CodeVersion: "example"},
		NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		Execute:   fetchPriceWorkflow,
	})
	price, err := handler(context.Background(), wrapperspb.String("AAPL"))
	if err != nil {
		panic(err)
	}

	fmt.Println(price.GetValue())
}

func fetchPriceWorkflow(ctx context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	fetch := workflow.WrapActivity(workflow.ActivityWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
		Options:   &workflow.ActivityOptions{ActivityId: "fetch:aapl"},
		NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		Execute: func(_ context.Context, input *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			return wrapperspb.String(input.GetValue() + " 100.00"), nil
		},
	})
	return fetch(ctx, request)
}
