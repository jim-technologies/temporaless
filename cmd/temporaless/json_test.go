package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	temporalessv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestMarshalCLIProtoApplicationAny(t *testing.T) {
	tests := []struct {
		name    string
		message proto.Message
		fields  map[string]*anypb.Any
	}{
		{
			name: "workflow input and result",
			message: &temporalessv1.WorkflowRecord{
				WorkflowType: "example.v1.RunRequest->example.v1.RunResponse",
				Input:        &anypb.Any{TypeUrl: "type.googleapis.com/example.v1.RunRequest", Value: []byte{0x08, 0x96, 0x01}},
				Result:       &anypb.Any{},
			},
			fields: map[string]*anypb.Any{
				"input":  {TypeUrl: "type.googleapis.com/example.v1.RunRequest", Value: []byte{0x08, 0x96, 0x01}},
				"result": {},
			},
		},
		{
			name: "activity input and result",
			message: &temporalessv1.ActivityRecord{
				ActivityType: "example.v1.FetchRequest->example.v1.FetchResponse",
				Input:        &anypb.Any{TypeUrl: "type.googleapis.com/example.v1.FetchRequest", Value: []byte("request")},
				Result:       &anypb.Any{TypeUrl: "type.googleapis.com/example.v1.FetchResponse", Value: []byte("response")},
			},
			fields: map[string]*anypb.Any{
				"input":  {TypeUrl: "type.googleapis.com/example.v1.FetchRequest", Value: []byte("request")},
				"result": {TypeUrl: "type.googleapis.com/example.v1.FetchResponse", Value: []byte("response")},
			},
		},
		{
			name: "event payload",
			message: &temporalessv1.EventRecord{
				Payload: &anypb.Any{TypeUrl: "type.googleapis.com/example.v1.Approval", Value: []byte{0xde, 0xad, 0xbe, 0xef}},
			},
			fields: map[string]*anypb.Any{
				"payload": {TypeUrl: "type.googleapis.com/example.v1.Approval", Value: []byte{0xde, 0xad, 0xbe, 0xef}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := proto.Clone(test.message)
			data, err := marshalCLIProto(test.message)
			require.NoError(t, err)
			require.True(t, proto.Equal(original, test.message), "marshalCLIProto mutated its input")

			var object map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(data, &object))
			for field, want := range test.fields {
				var got cliAny
				require.NoError(t, json.Unmarshal(object[field], &got))
				require.Equal(t, want.GetTypeUrl(), got.TypeURL)
				require.Equal(t, base64.StdEncoding.EncodeToString(want.GetValue()), got.ValueBase64)
				require.NotContains(t, string(object[field]), "@type")
			}
		})
	}
}

func TestMarshalCLIProtoPreservesAbsentAny(t *testing.T) {
	data, err := marshalCLIProto(&temporalessv1.WorkflowRecord{WorkflowType: "example.v1.RunRequest->example.v1.RunResponse"})
	require.NoError(t, err)

	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &object))
	require.NotContains(t, object, "input")
	require.NotContains(t, object, "result")
	require.JSONEq(t, `{"workflowType":"example.v1.RunRequest->example.v1.RunResponse"}`, string(data))
}

func TestCLIJSONCommandsPreserveUnknownApplicationAny(t *testing.T) {
	root, store := newTestRoot(t)
	typeURL, payload := seedDescribeRun(t, store, "wf-1", "run-1")
	tests := []struct {
		name string
		args []string
	}{
		{
			name: "get workflow",
			args: []string{"get-workflow", "--workflow-id", "wf-1", "--run-id", "run-1"},
		},
		{
			name: "list workflows",
			args: []string{"list-workflows", "--workflow-id", "wf-1"},
		},
		{
			name: "export workflows",
			args: []string{"export", "--kind", "workflow", "--workflow-id", "wf-1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := append([]string{"--store-root", root, "--json"}, test.args...)
			var stdout, stderr bytes.Buffer
			require.NoError(t, run(context.Background(), args, &stdout, &stderr))
			require.Contains(t, stdout.String(), typeURL)
			require.Contains(t, stdout.String(), base64.StdEncoding.EncodeToString(payload))
			require.Contains(t, stdout.String(), `"valueBase64"`)
			require.False(t, strings.Contains(stdout.String(), `"@type"`))
		})
	}
}
