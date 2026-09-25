package inspection

import (
	"errors"
	"fmt"
	"os"
	"strings"

	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// DefaultRenderLimitBytes caps the packed size of a payload the renderer
// decodes or returns. Larger payloads report only their type and size.
const DefaultRenderLimitBytes = 1 << 20

// PayloadRenderer turns Any payloads into ProtoJSON for display. It always
// knows the well-known types and the types linked into the binary; an
// application's own types come from a FileDescriptorSet the operator
// configures. Unknown types fall back to their opaque bytes, the same
// descriptor-free representation the operator CLI's describe-run uses.
type PayloadRenderer struct {
	types      *resolver
	limitBytes int
}

// NewPayloadRenderer builds a renderer. descriptors may be nil (well-known and
// linked types only). limitBytes <= 0 selects DefaultRenderLimitBytes.
func NewPayloadRenderer(descriptors *descriptorpb.FileDescriptorSet, limitBytes int) (*PayloadRenderer, error) {
	if limitBytes <= 0 {
		limitBytes = DefaultRenderLimitBytes
	}
	local := new(protoregistry.Files)
	for _, file := range descriptors.GetFile() {
		if _, err := protoregistry.GlobalFiles.FindFileByPath(file.GetName()); err == nil {
			// Linked into this binary already (well-known types, the
			// Temporaless records); the generated types win.
			continue
		}
		descriptor, err := protodesc.NewFile(file, filesResolver{local: local})
		if err != nil {
			return nil, fmt.Errorf("payload descriptors: %s: %w", file.GetName(), err)
		}
		if err := local.RegisterFile(descriptor); err != nil {
			return nil, fmt.Errorf("payload descriptors: %s: %w", file.GetName(), err)
		}
	}
	return &PayloadRenderer{
		types:      &resolver{local: dynamicpb.NewTypes(local)},
		limitBytes: limitBytes,
	}, nil
}

// LoadDescriptorSet reads a binary FileDescriptorSet, such as the output of
// `buf build -o descriptors.binpb`.
func LoadDescriptorSet(path string) (*descriptorpb.FileDescriptorSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	set := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(data, set); err != nil {
		return nil, fmt.Errorf("decode descriptor set %s: %w", path, err)
	}
	return set, nil
}

// Render prepares one payload for display. When visible is false only the
// type URL and size are returned.
func (renderer *PayloadRenderer) Render(path string, payload *anypb.Any, visible bool) *inspectionv1.RenderedPayload {
	rendered := &inspectionv1.RenderedPayload{
		Path:      path,
		TypeUrl:   payload.GetTypeUrl(),
		SizeBytes: uint64(len(payload.GetValue())),
	}
	if !visible {
		rendered.Redacted = true
		return rendered
	}
	if len(payload.GetValue()) > renderer.limitBytes {
		return rendered
	}
	if value, err := renderer.renderJSON(payload); err == nil {
		rendered.Value = &inspectionv1.RenderedPayload_Json{Json: value}
		return rendered
	}
	rendered.Value = &inspectionv1.RenderedPayload_Opaque{Opaque: payload.GetValue()}
	return rendered
}

func (renderer *PayloadRenderer) renderJSON(payload *anypb.Any) (*structpb.Value, error) {
	message, err := anypb.UnmarshalNew(payload, proto.UnmarshalOptions{Resolver: renderer.types})
	if err != nil {
		return nil, err
	}
	data, err := protojson.MarshalOptions{Resolver: renderer.types}.Marshal(message)
	if err != nil {
		return nil, err
	}
	value := &structpb.Value{}
	if err := protojson.Unmarshal(data, value); err != nil {
		return nil, err
	}
	return value, nil
}

// resolver answers from the configured descriptors first and the types linked
// into the binary second.
type resolver struct {
	local *dynamicpb.Types
}

func (types *resolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	if messageType, err := protoregistry.GlobalTypes.FindMessageByName(name); err == nil {
		return messageType, nil
	}
	return types.local.FindMessageByName(name)
}

func (types *resolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	name := url
	if slash := strings.LastIndexByte(url, '/'); slash >= 0 {
		name = url[slash+1:]
	}
	return types.FindMessageByName(protoreflect.FullName(name))
}

func (types *resolver) FindExtensionByName(name protoreflect.FullName) (protoreflect.ExtensionType, error) {
	if extension, err := protoregistry.GlobalTypes.FindExtensionByName(name); err == nil {
		return extension, nil
	}
	return types.local.FindExtensionByName(name)
}

func (types *resolver) FindExtensionByNumber(message protoreflect.FullName, number protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	if extension, err := protoregistry.GlobalTypes.FindExtensionByNumber(message, number); err == nil {
		return extension, nil
	}
	return types.local.FindExtensionByNumber(message, number)
}

// filesResolver resolves imports against the descriptors registered so far
// and then against the files linked into the binary.
type filesResolver struct {
	local *protoregistry.Files
}

func (files filesResolver) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if file, err := files.local.FindFileByPath(path); err == nil {
		return file, nil
	}
	return protoregistry.GlobalFiles.FindFileByPath(path)
}

func (files filesResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	descriptor, err := files.local.FindDescriptorByName(name)
	if err == nil {
		return descriptor, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}
