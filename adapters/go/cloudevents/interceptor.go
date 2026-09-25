// Package cloudevents publishes best-effort record invalidations from a
// RecordStoreService server. Downstream consumers own logging and indexing.
package cloudevents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"unicode"

	"connectrpc.com/connect"
	"github.com/cloudevents/sdk-go/v2/event"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/proto"
)

// Options contains the application-owned publication boundary. NewID must be
// concurrency-safe and return a new observation ID unique within Source.
// Publish must also be concurrency-safe, bound its I/O, and preserve the
// event ID on delivery retries.
// Publication errors are logged without changing a successful storage result.
type Options struct {
	Source  string
	NewID   func() string
	Publish func(context.Context, event.Event) error
}

// NewInterceptor observes successful mutating RPC calls, including idempotent
// repeats. Install it on the RecordStoreService server, inside authorization.
// It does not observe direct store calls, repairs inside DueTimers, or partial
// writes from failed RPCs. Reconciliation is required; this is not an audit log.
func NewInterceptor(options Options) (connect.Interceptor, error) {
	if options.NewID == nil || options.Publish == nil {
		return nil, errors.New("CloudEvents source, NewID, and Publish are required")
	}
	source, sourceErr := url.Parse(options.Source)
	_, escapeErr := url.PathUnescape(options.Source)
	if sourceErr != nil || escapeErr != nil || source.Scheme == "" || strings.IndexFunc(options.Source, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) >= 0 {
		return nil, errors.New("CloudEvents source must be an absolute URI without whitespace or controls")
	}
	probe := event.New(event.CloudEventsVersionV1)
	probe.SetID("validate")
	probe.SetSource(options.Source)
	probe.SetType("validate")
	if err := probe.Validate(); err != nil {
		return nil, err
	}
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			if request.Spec().IsClient {
				return next(ctx, request)
			}
			key, eventType := observation(request)
			var payload []byte
			var payloadErr error
			if key != nil {
				// Serialize before calling the handler so later mutation cannot
				// change which identity this invocation observed.
				key = proto.Clone(key)
				key.ProtoReflect().SetUnknown(nil)
				payload, payloadErr = proto.MarshalOptions{Deterministic: true}.Marshal(key)
			}
			response, err := next(ctx, request)
			if err != nil || key == nil {
				return response, err
			}
			if payloadErr == nil {
				payloadErr = publish(ctx, options, eventType, key, payload)
			}
			if payloadErr != nil {
				slog.ErrorContext(ctx, "CloudEvent publication failed; storage RPC succeeded; reconcile downstream projections", "type", eventType, "error", payloadErr)
			}
			return response, nil
		}
	}), nil
}

func observation(request connect.AnyRequest) (proto.Message, string) {
	service := temporalessv1.File_temporaless_v1_temporaless_proto.Services().ByName("RecordStoreService")
	for i := 0; i < service.Methods().Len(); i++ {
		method := service.Methods().Get(i)
		if request.Spec().Procedure != "/"+string(service.FullName())+"/"+string(method.Name()) {
			continue
		}
		message, ok := request.Any().(proto.Message)
		if !ok || message.ProtoReflect().Descriptor().FullName() != method.Input().FullName() {
			return nil, ""
		}
		// The supported mutation request shapes are generated protobufs.
		// Never forward records, Any payloads, annotations, or owner IDs.
		var key proto.Message
		switch value := message.(type) {
		case *temporalessv1.PutWorkflowRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.PutActivityRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.PutTimerRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.PutEventRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.DeliverEventRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.TryCreateClaimRequest:
			key = value.GetRecord().GetKey()
		case *temporalessv1.DeleteWorkflowRequest:
			key = value.GetKey()
		case *temporalessv1.DeleteActivityRequest:
			key = value.GetKey()
		case *temporalessv1.DeleteTimerRequest:
			key = value.GetKey()
		case *temporalessv1.DeleteEventRequest:
			key = value.GetKey()
		case *temporalessv1.DeleteClaimRequest:
			key = value.GetKey()
		case *temporalessv1.DeleteRunRequest:
			key = value.GetKey()
		}
		if key == nil || !key.ProtoReflect().IsValid() {
			return nil, ""
		}
		return key, string(method.FullName())
	}
	return nil, ""
}

// Recover only publication callbacks: a publisher panic must not turn a
// committed storage operation into an RPC failure. Storage panics still escape.
func publish(ctx context.Context, options Options, eventType string, key proto.Message, payload []byte) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("CloudEvents publication panicked: %v", recovered)
		}
	}()
	observed := event.New(event.CloudEventsVersionV1)
	id := options.NewID()
	if id == "" || strings.TrimSpace(id) != id || strings.IndexFunc(id, unicode.IsControl) >= 0 {
		return errors.New("CloudEvents ID must be nonempty without surrounding whitespace or controls")
	}
	observed.SetID(id)
	observed.SetSource(options.Source)
	observed.SetType(eventType)
	observed.SetDataSchema("https://type.googleapis.com/" + string(key.ProtoReflect().Descriptor().FullName()))
	if err := observed.SetData("application/protobuf", payload); err != nil {
		return err
	}
	if err := observed.Validate(); err != nil {
		return err
	}
	return options.Publish(ctx, observed)
}
