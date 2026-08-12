package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// cliAny is the descriptor-free representation of an application payload.
// Keeping the original Any bytes makes CLI output useful even when the
// application protobuf descriptors are not linked into this operator binary.
type cliAny struct {
	TypeURL     string `json:"typeUrl"`
	ValueBase64 string `json:"valueBase64"`
}

// marshalCLIProto uses normal protojson for framework fields and represents
// application Any payloads as their type URL plus opaque protobuf bytes.
// Unlike protojson's standard Any encoding, this does not require the CLI to
// know every application's protobuf descriptors.
func marshalCLIProto(message proto.Message) ([]byte, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, errors.New("marshal CLI proto: message is nil")
	}

	cloned := proto.Clone(message)
	applicationPayloads := make(map[string]*anypb.Any, 2)
	switch record := cloned.(type) {
	case *temporalessv1.WorkflowRecord:
		preserveCLIAny(applicationPayloads, "input", record.Input)
		preserveCLIAny(applicationPayloads, "result", record.Result)
		record.Input = nil
		record.Result = nil
	case *temporalessv1.ActivityRecord:
		preserveCLIAny(applicationPayloads, "input", record.Input)
		preserveCLIAny(applicationPayloads, "result", record.Result)
		record.Input = nil
		record.Result = nil
	case *temporalessv1.EventRecord:
		preserveCLIAny(applicationPayloads, "payload", record.Payload)
		record.Payload = nil
	}

	data, err := protojson.Marshal(cloned)
	if err != nil {
		return nil, err
	}
	if len(applicationPayloads) == 0 {
		return data, nil
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, err
	}
	for field, payload := range applicationPayloads {
		encoded, err := json.Marshal(cliAny{
			TypeURL:     payload.GetTypeUrl(),
			ValueBase64: base64.StdEncoding.EncodeToString(payload.GetValue()),
		})
		if err != nil {
			return nil, err
		}
		object[field] = encoded
	}
	return json.Marshal(object)
}

func preserveCLIAny(payloads map[string]*anypb.Any, field string, payload *anypb.Any) {
	if payload != nil {
		payloads[field] = payload
	}
}
