package console

import (
	"bytes"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

var (
	payloadTypesMu sync.Mutex
	// payloadFiles holds each registered file's definition without source
	// info, so a second store can register the same file but never a
	// different one under the same name.
	payloadFiles = map[string][]byte{}
)

// RegisterPayloadTypes makes the messages in an application's descriptor set
// resolvable process-wide (protoregistry.GlobalFiles and GlobalTypes). The
// Invariant Protocol projections marshal responses with the default ProtoJSON
// resolver, so this is what lets DescribeRun over Connect/HTTP JSON, MCP, and
// the CLI show a stored payload of these types as typed ProtoJSON instead of
// an OpaquePayload. Call it once per store's descriptor set, before serving.
//
// Files linked into the binary (well-known types, the Temporaless records)
// keep their generated definitions. A file registered by an earlier call must
// be identical apart from source comments: the registry is shared by every
// store, so two stores that disagree on one file are rejected instead of one
// silently rendering with the other's schema. Files must be in dependency
// order, as `buf build` and `protoc --include_imports` write them.
func RegisterPayloadTypes(descriptors *descriptorpb.FileDescriptorSet) error {
	payloadTypesMu.Lock()
	defer payloadTypesMu.Unlock()
	for _, file := range descriptors.GetFile() {
		name := file.GetName()
		stripped := proto.Clone(file).(*descriptorpb.FileDescriptorProto)
		stripped.SourceCodeInfo = nil
		definition, err := proto.MarshalOptions{Deterministic: true}.Marshal(stripped)
		if err != nil {
			return fmt.Errorf("payload descriptors: %s: %w", name, err)
		}
		if previous, ok := payloadFiles[name]; ok {
			if !bytes.Equal(previous, definition) {
				return fmt.Errorf("payload descriptors: %s is already registered with a different definition; "+
					"payload types are resolved process-wide, so every store must agree on it", name)
			}
			continue
		}
		if _, err := protoregistry.GlobalFiles.FindFileByPath(name); err == nil {
			continue
		}
		descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
		if err != nil {
			return fmt.Errorf("payload descriptors: %s: %w", name, err)
		}
		// The global registries panic on a name conflict, so a conflict is
		// found and reported here first.
		if err := globalConflict(descriptor); err != nil {
			return fmt.Errorf("payload descriptors: %s: %w", name, err)
		}
		if err := protoregistry.GlobalFiles.RegisterFile(descriptor); err != nil {
			return fmt.Errorf("payload descriptors: %s: %w", name, err)
		}
		if err := registerMessages(descriptor.Messages()); err != nil {
			return fmt.Errorf("payload descriptors: %s: %w", name, err)
		}
		payloadFiles[name] = definition
	}
	return nil
}

func registerMessages(messages protoreflect.MessageDescriptors) error {
	for index := range messages.Len() {
		message := messages.Get(index)
		if message.IsMapEntry() {
			continue
		}
		if err := protoregistry.GlobalTypes.RegisterMessage(dynamicpb.NewMessageType(message)); err != nil {
			return err
		}
		if err := registerMessages(message.Messages()); err != nil {
			return err
		}
	}
	return nil
}

// globalConflict reports a declaration of file whose full name the global
// registries already hold, or a package name that another kind of
// declaration already uses.
func globalConflict(file protoreflect.FileDescriptor) error {
	taken := func(name protoreflect.FullName) error {
		if _, err := protoregistry.GlobalFiles.FindDescriptorByName(name); err == nil {
			return fmt.Errorf("%s is already declared by another file", name)
		}
		return nil
	}
	for name := file.Package(); name != ""; name = name.Parent() {
		if err := taken(name); err != nil {
			return err
		}
	}
	var names []protoreflect.FullName
	for index := range file.Messages().Len() {
		names = append(names, file.Messages().Get(index).FullName())
	}
	for index := range file.Enums().Len() {
		enum := file.Enums().Get(index)
		names = append(names, enum.FullName())
		for value := range enum.Values().Len() {
			names = append(names, enum.Values().Get(value).FullName())
		}
	}
	for index := range file.Extensions().Len() {
		names = append(names, file.Extensions().Get(index).FullName())
	}
	for index := range file.Services().Len() {
		names = append(names, file.Services().Get(index).FullName())
	}
	for _, name := range names {
		if err := taken(name); err != nil {
			return err
		}
	}
	return messageTypeConflict(file.Messages())
}

func messageTypeConflict(messages protoreflect.MessageDescriptors) error {
	for index := range messages.Len() {
		message := messages.Get(index)
		if _, err := protoregistry.GlobalTypes.FindMessageByName(message.FullName()); err == nil {
			return fmt.Errorf("message type %s is already registered", message.FullName())
		}
		if err := messageTypeConflict(message.Messages()); err != nil {
			return err
		}
	}
	return nil
}
