package storage

import (
	"encoding/json"
	"os"
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestFencedExecutionContract(t *testing.T) {
	data, err := os.ReadFile("../../../testdata/fenced-execution-contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name    string
		Type    string
		Message json.RawMessage
		Valid   bool
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.Name, func(t *testing.T) {
			kind, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName("temporaless.v1." + test.Type))
			if err != nil {
				t.Fatal(err)
			}
			message := kind.New().Interface()
			if err := protojson.Unmarshal(test.Message, message); err != nil {
				t.Fatal(err)
			}
			err = protovalidate.Validate(message)
			if (err == nil) != test.Valid {
				t.Fatalf("valid=%v: %v", test.Valid, err)
			}
		})
	}
}
