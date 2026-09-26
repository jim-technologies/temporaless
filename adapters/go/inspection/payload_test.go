package inspection_test

import (
	"context"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// orderDescriptors is an application descriptor set the binary does not link:
// app.v1.Order { string id = 1; int64 quantity = 2; }.
func orderDescriptors(t *testing.T) *descriptorpb.FileDescriptorSet {
	t.Helper()
	return &descriptorpb.FileDescriptorSet{File: []*descriptorpb.FileDescriptorProto{{
		Name:    proto.String("app/v1/order.proto"),
		Package: proto.String("app.v1"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("Order"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String("id")},
				{Name: proto.String("quantity"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), JsonName: proto.String("quantity")},
			},
		}},
	}}}
}

func orderPayload(t *testing.T, set *descriptorpb.FileDescriptorSet) *anypb.Any {
	t.Helper()
	files, err := protodesc.NewFiles(set)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := files.FindDescriptorByName("app.v1.Order")
	if err != nil {
		t.Fatal(err)
	}
	order := dynamicpb.NewMessage(descriptor.(protoreflect.MessageDescriptor))
	if err := protojson.Unmarshal([]byte(`{"id":"o-1","quantity":"3"}`), order); err != nil {
		t.Fatal(err)
	}
	value, err := proto.Marshal(order)
	if err != nil {
		t.Fatal(err)
	}
	return &anypb.Any{TypeUrl: "type.googleapis.com/app.v1.Order", Value: value}
}

func TestPayloadRenderer(t *testing.T) {
	set := orderDescriptors(t)
	withDescriptors, err := inspection.NewPayloadRenderer(set, 0)
	if err != nil {
		t.Fatal(err)
	}
	wellKnownOnly, err := inspection.NewPayloadRenderer(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	tiny, err := inspection.NewPayloadRenderer(set, 4)
	if err != nil {
		t.Fatal(err)
	}
	stringValue, err := anypb.New(wrapperspb.String("20260925T080000"))
	if err != nil {
		t.Fatal(err)
	}
	structValue, err := anypb.New(&structpb.Struct{Fields: map[string]*structpb.Value{"pages": structpb.NewNumberValue(6)}})
	if err != nil {
		t.Fatal(err)
	}
	order := orderPayload(t, set)

	tests := []struct {
		name     string
		renderer *inspection.PayloadRenderer
		payload  *anypb.Any
		visible  bool
		wantJSON string
		opaque   bool
		redacted bool
	}{
		{name: "well-known wrapper", renderer: wellKnownOnly, payload: stringValue, visible: true, wantJSON: `"20260925T080000"`},
		{name: "well-known struct", renderer: wellKnownOnly, payload: structValue, visible: true, wantJSON: `{"pages":6}`},
		{name: "application type from descriptors", renderer: withDescriptors, payload: order, visible: true, wantJSON: `{"id":"o-1","quantity":"3"}`},
		{name: "application type without descriptors", renderer: wellKnownOnly, payload: order, visible: true, opaque: true},
		{name: "redacted", renderer: withDescriptors, payload: order, visible: false, redacted: true},
		{name: "over the render limit", renderer: tiny, payload: order, visible: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rendered := test.renderer.Render("workflow.input", test.payload, test.visible)
			if rendered.GetTypeUrl() != test.payload.GetTypeUrl() || rendered.GetSizeBytes() != uint64(len(test.payload.GetValue())) {
				t.Fatalf("header = %s/%d", rendered.GetTypeUrl(), rendered.GetSizeBytes())
			}
			if rendered.GetRedacted() != test.redacted {
				t.Fatalf("redacted = %t", rendered.GetRedacted())
			}
			if test.wantJSON != "" {
				got, err := protojson.Marshal(rendered.GetJson())
				if err != nil {
					t.Fatal(err)
				}
				if !jsonEqual(t, string(got), test.wantJSON) {
					t.Fatalf("json = %s, want %s", got, test.wantJSON)
				}
				return
			}
			if test.opaque != (rendered.GetOpaque() != nil) {
				t.Fatalf("opaque = %v, want %t", rendered.GetOpaque(), test.opaque)
			}
			if rendered.GetJson() != nil {
				t.Fatalf("unexpected json %v", rendered.GetJson())
			}
		})
	}
}

func TestDescribeRunRedactsPayloads(t *testing.T) {
	f := newFixture(t)
	run := key("pull:weather", "20260925T080000")
	f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -time.Hour, duration(-time.Minute))
	f.event(run, "approval", -30*time.Minute)
	viewerWithoutPayloads := func(context.Context, *inspection.Store) (inspection.Grant, bool, error) {
		return inspection.Grant{Payloads: false}, true, nil
	}
	response, err := f.service(inspection.Options{Access: viewerWithoutPayloads}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetPayloadVisibility() != inspectionv1.PayloadVisibility_PAYLOAD_VISIBILITY_REDACTED {
		t.Fatalf("visibility = %s", response.GetPayloadVisibility())
	}
	for _, payload := range []*anypb.Any{response.GetWorkflow().GetInput(), response.GetWorkflow().GetResult(), response.GetEvents()[0].GetPayload()} {
		opaque := &inspectionv1.OpaquePayload{}
		if err := payload.UnmarshalTo(opaque); err != nil {
			t.Fatalf("redacted payload is %s, want an OpaquePayload: %v", payload.GetTypeUrl(), err)
		}
		if !opaque.GetRedacted() || opaque.GetTypeUrl() == "" || len(opaque.GetValue()) != 0 {
			t.Fatalf("redacted payload = %v, want the type URL only", opaque)
		}
	}
	paths := map[string]bool{}
	for _, rendered := range response.GetPayloads() {
		paths[rendered.GetPath()] = rendered.GetRedacted() && rendered.GetSizeBytes() > 0 && rendered.GetValue() == nil
	}
	for _, path := range []string{"workflow.input", "workflow.result", "event/approval.payload"} {
		if !paths[path] {
			t.Fatalf("rendered payloads = %v, want %s redacted with its size", paths, path)
		}
	}

	// The stored record keeps its payload: redaction is a view, not a write.
	stored, _, err := f.records.GetWorkflow(context.Background(), run)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.GetInput().GetValue()) == 0 {
		t.Fatal("redaction modified the stored record")
	}
}

func jsonEqual(t *testing.T, left, right string) bool {
	t.Helper()
	leftValue, rightValue := &structpb.Value{}, &structpb.Value{}
	if err := protojson.Unmarshal([]byte(left), leftValue); err != nil {
		t.Fatal(err)
	}
	if err := protojson.Unmarshal([]byte(right), rightValue); err != nil {
		t.Fatal(err)
	}
	return proto.Equal(leftValue, rightValue)
}

// TestDescribeRunRecordPayloads covers what the response records carry for
// each stored payload: the stored Any when ProtoJSON can render it with the
// process-wide types, otherwise an OpaquePayload holding the stored bytes.
func TestDescribeRunRecordPayloads(t *testing.T) {
	set := orderDescriptors(t)
	order := orderPayload(t, set)
	wellKnown, err := anypb.New(wrapperspb.String("20260925T080000"))
	if err != nil {
		t.Fatal(err)
	}
	corrupt := &anypb.Any{TypeUrl: wellKnown.GetTypeUrl(), Value: []byte{0xff, 0xff}}
	tests := []struct {
		name       string
		payload    *anypb.Any
		wantStored bool
	}{
		{"well-known type stays as stored", wellKnown, true},
		{"application type outside the process registry is opaque", order, false},
		{"undecodable bytes of a known type are opaque", corrupt, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFixture(t)
			// Descriptors on the store render the payload for display; the
			// record keeps the opaque stand-in because the process-wide
			// registry, which the JSON projections use, does not know it.
			renderer, err := inspection.NewPayloadRenderer(set, 0)
			if err != nil {
				t.Fatal(err)
			}
			f.store.Payloads = renderer
			run := key("app:orders", "r1")
			record := f.workflow(run, temporalessv1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, -time.Hour, duration(-time.Minute))
			record.Input = test.payload
			if err := f.records.PutWorkflow(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			response, err := f.service(inspection.Options{}).DescribeRun(context.Background(), &inspectionv1.DescribeRunRequest{Store: "engine", Key: run.Proto()})
			if err != nil {
				t.Fatal(err)
			}
			input := response.GetWorkflow().GetInput()
			if test.wantStored {
				if !proto.Equal(input, test.payload) {
					t.Fatalf("input = %v, want the stored payload", input)
				}
			} else {
				opaque := &inspectionv1.OpaquePayload{}
				if err := input.UnmarshalTo(opaque); err != nil {
					t.Fatalf("input is %s, want an OpaquePayload: %v", input.GetTypeUrl(), err)
				}
				if opaque.GetRedacted() || opaque.GetTypeUrl() != test.payload.GetTypeUrl() || string(opaque.GetValue()) != string(test.payload.GetValue()) {
					t.Fatalf("opaque = %v, want the stored type URL and bytes", opaque)
				}
			}
			if _, err := protojson.Marshal(response); err != nil {
				t.Fatalf("the response does not marshal as ProtoJSON: %v", err)
			}
		})
	}
}
