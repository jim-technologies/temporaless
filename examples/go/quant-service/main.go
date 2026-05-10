// Canonical ConnectRPC-shaped service example.
//
// The framework's design tenet: a workflow IS a normal connect handler. You
// write the standard ConnectRPC method shape
//
//	func (s *Service) Method(
//	    ctx context.Context, req *connect.Request[Req],
//	) (*connect.Response[Resp], error)
//
// and call workflow.HandleConnect inside it. Replay, idempotency, and
// persistence follow without changing the handler interface.
//
// To deploy this over real gRPC/ConnectRPC:
//
//  1. Define quant_signals.proto with your service and message types.
//  2. buf generate to produce the QuantServiceHandler interface and types.
//  3. Replace wrapperspb.StringValue below with your generated FetchRequest /
//     FetchResponse types.
//  4. Mount the service on any net/http or connectrpc mux:
//
//     mux := http.NewServeMux()
//     mux.Handle(quantsvcv1connect.NewQuantServiceHandler(service, connect.WithInterceptors(authInterceptor, traceInterceptor)))
//     http.ListenAndServe(":8080", h2c.NewHandler(mux, &http2.Server{}))
//
// This file uses wrapperspb.StringValue for the request/response so the
// example runs without proto codegen, but the structure is identical to what
// you'd write in production.
//
// Run with `go run ./examples/go/quant-service/`.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"github.com/jim-technologies/temporaless/core/go/workflow"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const codeVersion = "example"

// QuantService implements two ConnectRPC handlers. The handlers happen to be
// workflows — that's the only Temporaless-specific thing about them.
type QuantService struct {
	store storage.Store
	calls map[string]int
}

func newQuantService(store storage.Store) *QuantService {
	return &QuantService{store: store, calls: map[string]int{}}
}

// FetchPrices is a real ConnectRPC handler signature. The framework wraps it
// as a workflow keyed on prices:{symbol}. Two calls with the same symbol +
// run_id produce the same result without re-invoking the vendor. HandleConnect
// auto-maps framework errors to the right *connect.Error code.
func (s *QuantService) FetchPrices(
	ctx context.Context,
	req *connect.Request[wrapperspb.StringValue],
) (*connect.Response[wrapperspb.StringValue], error) {
	return workflow.HandleConnect(
		ctx, req,
		workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Store: s.store,
			OptionsFor: func(_ context.Context, r *wrapperspb.StringValue) (*workflow.Options, error) {
				return &workflow.Options{
					WorkflowId:  "prices:" + r.GetValue(),
					RunId:       "2026-05-04",
					CodeVersion: codeVersion,
				}, nil
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			Execute:   s.fetchPricesBody,
		},
	)
}

// ComposeSignal is the second canonical handler — sequentially fans out to
// per-symbol activities inside one workflow. Each activity record is keyed
// independently, so partial failures only retry the symbol that failed.
func (s *QuantService) ComposeSignal(
	ctx context.Context,
	req *connect.Request[wrapperspb.StringValue],
) (*connect.Response[wrapperspb.StringValue], error) {
	return workflow.HandleConnect(
		ctx, req,
		workflow.WorkflowWrapOptions[*wrapperspb.StringValue, *wrapperspb.StringValue]{
			Store: s.store,
			OptionsFor: func(_ context.Context, r *wrapperspb.StringValue) (*workflow.Options, error) {
				return &workflow.Options{
					WorkflowId:  "signals:batch",
					RunId:       r.GetValue(),
					CodeVersion: codeVersion,
				}, nil
			},
			NewResult: func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			Execute:   s.composeSignalBody,
		},
	)
}

func (s *QuantService) fetchPricesBody(
	ctx context.Context, req *wrapperspb.StringValue,
) (*wrapperspb.StringValue, error) {
	return workflow.ExecuteActivity(
		ctx,
		&workflow.ActivityOptions{ActivityId: "vendor:" + req.GetValue()},
		req,
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		s.vendorFetch,
	)
}

func (s *QuantService) composeSignalBody(
	ctx context.Context, _ *wrapperspb.StringValue,
) (*wrapperspb.StringValue, error) {
	symbols := []string{"AAPL", "MSFT", "GOOG", "TSLA", "NVDA"}
	prices := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		out, err := workflow.ExecuteActivity(
			ctx,
			&workflow.ActivityOptions{ActivityId: "fetch:" + symbol},
			wrapperspb.String(symbol),
			func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
			s.vendorFetch,
		)
		if err != nil {
			return nil, err
		}
		prices = append(prices, out.GetValue())
	}
	return workflow.ExecuteActivity(
		ctx,
		&workflow.ActivityOptions{ActivityId: "compose:signal"},
		wrapperspb.String(strings.Join(prices, ",")),
		func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		s.composeFn,
	)
}

func (s *QuantService) vendorFetch(
	_ context.Context, req *wrapperspb.StringValue,
) (*wrapperspb.StringValue, error) {
	s.calls[req.GetValue()]++
	return wrapperspb.String(req.GetValue() + " 100.00"), nil
}

func (s *QuantService) composeFn(
	_ context.Context, req *wrapperspb.StringValue,
) (*wrapperspb.StringValue, error) {
	s.calls["__compose__"]++
	return wrapperspb.String("signal(" + req.GetValue() + ")"), nil
}

func main() {
	root, err := os.MkdirTemp("", "temporaless-quant-svc-")
	if err != nil {
		panic(err)
	}
	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		panic(err)
	}
	defer operator.Close()
	store := storage.NewOpenDALStore(operator)
	service := newQuantService(store)
	ctx := context.Background()

	fmt.Println("=== single-symbol workflow (FetchPrices) ===")
	resp, err := service.FetchPrices(ctx, connect.NewRequest(wrapperspb.String("AAPL")))
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q (vendor calls: %d)\n", resp.Msg.GetValue(), service.calls["AAPL"])

	fmt.Println("\n=== same call replays from storage ===")
	resp, err = service.FetchPrices(ctx, connect.NewRequest(wrapperspb.String("AAPL")))
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q (vendor calls: %d, no new vendor call)\n", resp.Msg.GetValue(), service.calls["AAPL"])

	fmt.Println("\n=== fan-out workflow (ComposeSignal) ===")
	signal, err := service.ComposeSignal(ctx, connect.NewRequest(wrapperspb.String("batch-1")))
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q (compose calls: %d)\n", signal.Msg.GetValue(), service.calls["__compose__"])

	fmt.Println("\n=== signal replay short-circuits all 5 fetches + compose ===")
	signal, err = service.ComposeSignal(ctx, connect.NewRequest(wrapperspb.String("batch-1")))
	if err != nil {
		panic(err)
	}
	fmt.Printf("  result: %q (compose calls: %d, no new compose call)\n", signal.Msg.GetValue(), service.calls["__compose__"])
}
