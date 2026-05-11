package outbox

import (
	"strings"
	"testing"

	"github.com/jim-technologies/temporaless/core/go/storage"
)

func TestDerive_DeterministicOverIdentity(t *testing.T) {
	a := Derive(storage.DefaultNamespace, "wf-a", "run-1", "act:1")
	b := Derive(storage.DefaultNamespace, "wf-a", "run-1", "act:1")
	if a != b {
		t.Fatalf("same identity must produce same key: %q vs %q", a, b)
	}
}

func TestDerive_DifferentIdentityProducesDifferentKey(t *testing.T) {
	cases := []struct {
		name      string
		namespace string
		workflow  string
		run       string
		activity  string
	}{
		{"base", "default", "wf-a", "run-1", "act:1"},
		{"different namespace", "tenant-b", "wf-a", "run-1", "act:1"},
		{"different workflow", "default", "wf-b", "run-1", "act:1"},
		{"different run", "default", "wf-a", "run-2", "act:1"},
		{"different activity", "default", "wf-a", "run-1", "act:2"},
	}
	keys := map[string]string{}
	for _, c := range cases {
		key := Derive(c.namespace, c.workflow, c.run, c.activity)
		if existing, ok := keys[key]; ok {
			t.Fatalf("collision between %q and the case producing %q", c.name, existing)
		}
		keys[key] = c.name
	}
}

func TestDerive_HasFrameworkPrefix(t *testing.T) {
	key := Derive("default", "wf", "run", "act")
	if !strings.HasPrefix(key, Prefix) {
		t.Fatalf("missing prefix %q in key %q", Prefix, key)
	}
}

func TestDerive_StableLength(t *testing.T) {
	// 16 hex bytes after the prefix → 32 hex chars + len(Prefix).
	got := Derive("default", "wf", "run", "act")
	want := len(Prefix) + 32
	if len(got) != want {
		t.Fatalf("len(key) = %d, want %d (key=%q)", len(got), want, got)
	}
}

func TestDerive_LongIDsStillFixedWidth(t *testing.T) {
	long := strings.Repeat("a", 200)
	short := Derive("default", "wf", "run", "act")
	full := Derive("default", long, long, long)
	if len(short) != len(full) {
		t.Fatalf("len(short)=%d len(long-input)=%d", len(short), len(full))
	}
}
