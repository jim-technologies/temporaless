package console_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/console"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
)

var clockNow = time.Date(2026, 9, 25, 8, 14, 0, 0, time.UTC)

func fixedNow() time.Time { return clockNow }

type signer struct {
	kid string
	ec  *ecdsa.PrivateKey
	rsa *rsa.PrivateKey
}

func newECSigner(t *testing.T, kid string) *signer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &signer{kid: kid, ec: key}
}

func newRSASigner(t *testing.T, kid string) *signer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return &signer{kid: kid, rsa: key}
}

func (s *signer) jwk() map[string]string {
	if s.ec != nil {
		x := make([]byte, 32)
		y := make([]byte, 32)
		s.ec.X.FillBytes(x)
		s.ec.Y.FillBytes(y)
		return map[string]string{"kty": "EC", "kid": s.kid, "use": "sig", "crv": "P-256",
			"x": base64.RawURLEncoding.EncodeToString(x), "y": base64.RawURLEncoding.EncodeToString(y)}
	}
	return map[string]string{"kty": "RSA", "kid": s.kid, "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(s.rsa.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(s.rsa.E)).Bytes())}
}

func jwks(t *testing.T, signers ...*signer) []byte {
	t.Helper()
	keys := make([]map[string]string, 0, len(signers))
	for _, s := range signers {
		keys = append(keys, s.jwk())
	}
	data, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (s *signer) token(t *testing.T, alg string, claims map[string]any) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": alg, "kid": s.kid, "typ": "JWT"})
	body, _ := json.Marshal(claims)
	input := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(body)
	digest := sha256.Sum256([]byte(input))
	var signature []byte
	if s.ec != nil {
		r, sv, err := ecdsa.Sign(rand.Reader, s.ec, digest[:])
		if err != nil {
			t.Fatal(err)
		}
		signature = make([]byte, 64)
		r.FillBytes(signature[:32])
		sv.FillBytes(signature[32:])
	} else {
		var err error
		if signature, err = rsa.SignPKCS1v15(rand.Reader, s.rsa, crypto.SHA256, digest[:]); err != nil {
			t.Fatal(err)
		}
	}
	return input + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func claims(overrides map[string]any) map[string]any {
	base := map[string]any{
		"iss":          "https://issuer.example.com",
		"aud":          []any{"temporaless-console"},
		"sub":          "user-7",
		"workspace_id": "ws-a",
		"exp":          float64(clockNow.Add(5 * time.Minute).Unix()),
		"iat":          float64(clockNow.Unix()),
	}
	for key, value := range overrides {
		if value == nil {
			delete(base, key)
		} else {
			base[key] = value
		}
	}
	return base
}

func writeFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestJWTVerifier(t *testing.T) {
	ec := newECSigner(t, "ec-1")
	other := newECSigner(t, "ec-1")
	rs := newRSASigner(t, "rsa-1")
	keys, err := console.NewJWKS("", writeFile(t, "jwks.json", jwks(t, ec, rs)), nil, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := console.NewJWTVerifier(console.JWTConfig{Issuer: "https://issuer.example.com", Audience: "temporaless-console"}, keys, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	unsigned := func() string {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","kid":"ec-1"}`))
		body, _ := json.Marshal(claims(nil))
		return header + "." + base64.RawURLEncoding.EncodeToString(body) + "."
	}
	tampered := func() string {
		parts := strings.Split(ec.token(t, "ES256", claims(nil)), ".")
		body, _ := json.Marshal(claims(map[string]any{"workspace_id": "ws-b"}))
		parts[1] = base64.RawURLEncoding.EncodeToString(body)
		return strings.Join(parts, ".")
	}
	tests := []struct {
		name          string
		token         string
		wantWorkspace string
	}{
		{"valid ES256", ec.token(t, "ES256", claims(nil)), "ws-a"},
		{"valid RS256", rs.token(t, "RS256", claims(nil)), "ws-a"},
		{"audience as a string", ec.token(t, "ES256", claims(map[string]any{"aud": "temporaless-console"})), "ws-a"},
		{"expiry within leeway", ec.token(t, "ES256", claims(map[string]any{"exp": float64(clockNow.Add(-10 * time.Second).Unix())})), "ws-a"},
		{"wrong issuer", ec.token(t, "ES256", claims(map[string]any{"iss": "https://other.example.com"})), ""},
		{"wrong audience", ec.token(t, "ES256", claims(map[string]any{"aud": []any{"another-product"}})), ""},
		{"expired", ec.token(t, "ES256", claims(map[string]any{"exp": float64(clockNow.Add(-time.Minute).Unix())})), ""},
		{"missing expiry", ec.token(t, "ES256", claims(map[string]any{"exp": nil})), ""},
		{"not yet valid", ec.token(t, "ES256", claims(map[string]any{"nbf": float64(clockNow.Add(time.Minute).Unix())})), ""},
		{"missing workspace", ec.token(t, "ES256", claims(map[string]any{"workspace_id": nil})), ""},
		{"algorithm mismatch", ec.token(t, "RS256", claims(nil)), ""},
		{"alg none", unsigned(), ""},
		{"signed by another key with the same kid", other.token(t, "ES256", claims(nil)), ""},
		{"tampered claims", tampered(), ""},
		{"unknown kid", newECSigner(t, "ec-9").token(t, "ES256", claims(nil)), ""},
		{"not a JWT", "opaque-token", ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			principal, err := verifier.Authenticate(context.Background(), test.token)
			if test.wantWorkspace == "" {
				if !errors.Is(err, console.ErrUnauthenticated) {
					t.Fatalf("err = %v, want ErrUnauthenticated", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if principal.Workspace != test.wantWorkspace || principal.Subject != "user-7" || principal.Unscoped {
				t.Fatalf("principal = %+v", principal)
			}
		})
	}
}

func TestJWKSURLRefreshesOnRotation(t *testing.T) {
	first := newECSigner(t, "k1")
	second := newECSigner(t, "k2")
	var served atomic.Value
	served.Store(jwks(t, first))
	var fetches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		_, _ = w.Write(served.Load().([]byte))
	}))
	defer server.Close()
	now := clockNow
	keys, err := console.NewJWKS(server.URL, "", server.Client(), func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := console.NewJWTVerifier(console.JWTConfig{Issuer: "https://issuer.example.com", Audience: "temporaless-console"}, keys, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Authenticate(context.Background(), first.token(t, "ES256", claims(nil))); err != nil {
		t.Fatal(err)
	}
	served.Store(jwks(t, first, second))
	// An unknown kid inside the refresh floor does not hammer the issuer.
	if _, err := verifier.Authenticate(context.Background(), second.token(t, "ES256", claims(nil))); err == nil {
		t.Fatal("rotated key accepted before the refresh floor")
	}
	now = now.Add(time.Minute)
	if _, err := verifier.Authenticate(context.Background(), second.token(t, "ES256", claims(map[string]any{"exp": float64(now.Add(time.Minute).Unix())}))); err != nil {
		t.Fatalf("rotated key after refresh floor: %v", err)
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2", got)
	}
}

func TestOpenFGA(t *testing.T) {
	var calls atomic.Int32
	var lastBody map[string]any
	var lastAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/stores/01STORE/check" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		lastAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&lastBody)
		tuple := lastBody["tuple_key"].(map[string]any)
		_ = json.NewEncoder(w).Encode(map[string]bool{"allowed": tuple["user"] == "user:user-7" && tuple["relation"] == "can_read"})
	}))
	defer server.Close()
	fga, err := console.NewOpenFGA(console.OpenFGAConfig{
		APIURL: server.URL, StoreID: "01STORE", AuthorizationModelID: "01MODEL",
		TokenFile: writeFile(t, "fga-token", []byte("fga-secret\n")),
	}, server.Client(), fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		user, relation string
		want           bool
	}{
		{"user:user-7", "can_read", true},
		{"user:user-7", "can_write", false},
		{"user:user-8", "can_read", false},
		{"user:user-7", "can_read", true},
	}
	for _, test := range tests {
		allowed, err := fga.Check(context.Background(), test.user, test.relation, "workspace:ws-a")
		if err != nil || allowed != test.want {
			t.Fatalf("Check(%s, %s) = %t, %v; want %t", test.user, test.relation, allowed, err, test.want)
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3 (the repeated check is cached)", calls.Load())
	}
	if lastAuth != "Bearer fga-secret" || lastBody["authorization_model_id"] != "01MODEL" {
		t.Fatalf("request auth=%q body=%v", lastAuth, lastBody)
	}

	server.Close()
	unreachable, err := console.NewOpenFGA(console.OpenFGAConfig{APIURL: server.URL, StoreID: "01STORE"}, nil, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := unreachable.Check(context.Background(), "user:user-7", "can_read", "workspace:ws-a"); err == nil || allowed {
		t.Fatalf("unreachable OpenFGA allowed=%t err=%v, want a denying error", allowed, err)
	}
}

type authorizerFunc func(ctx context.Context, user, relation, object string) (bool, error)

func (f authorizerFunc) Check(ctx context.Context, user, relation, object string) (bool, error) {
	return f(ctx, user, relation, object)
}

func TestScopedAccess(t *testing.T) {
	// The editor also holds every relation on ws-b, so for a ws-b principal
	// only the scope's workspace check can keep ws-a's store hidden.
	relations := map[string]bool{
		"user:viewer|can_read|workspace:ws-a": true, "user:editor|can_read|workspace:ws-a": true, "user:editor|can_write|workspace:ws-a": true,
		"user:editor|can_read|workspace:ws-b": true, "user:editor|can_write|workspace:ws-b": true,
	}
	authorizer := authorizerFunc(func(_ context.Context, user, relation, object string) (bool, error) {
		if user == "user:broken" {
			return false, errors.New("openfga down")
		}
		return relations[user+"|"+relation+"|"+object], nil
	})
	access := console.ScopedAccess(console.ScopedAccessConfig{Scopes: map[string]console.StoreScope{
		"engine":  {Workspace: "ws-a", PayloadRelation: "can_write"},
		"compute": {Workspace: "ws-b"},
	}}, authorizer)
	engine := &inspection.Store{ID: "engine"}
	compute := &inspection.Store{ID: "compute"}
	unscoped := &inspection.Store{ID: "unregistered"}
	tests := []struct {
		name         string
		principal    *console.Principal
		store        *inspection.Store
		wantVisible  bool
		wantPayloads bool
		wantErr      bool
	}{
		{"no principal sees nothing", nil, engine, false, false, false},
		{"viewer sees the store without payloads", &console.Principal{Subject: "viewer", Workspace: "ws-a"}, engine, true, false, false},
		{"editor sees payloads", &console.Principal{Subject: "editor", Workspace: "ws-a"}, engine, true, true, false},
		{"another workspace never sees it, whatever it holds there", &console.Principal{Subject: "editor", Workspace: "ws-b"}, engine, false, false, false},
		{"that workspace sees its own store", &console.Principal{Subject: "editor", Workspace: "ws-b"}, compute, true, true, false},
		{"a member without the relation", &console.Principal{Subject: "stranger", Workspace: "ws-a"}, engine, false, false, false},
		{"a store with no scope is hidden from tenants", &console.Principal{Subject: "editor", Workspace: "ws-a"}, unscoped, false, false, false},
		{"authorization outage fails closed", &console.Principal{Subject: "broken", Workspace: "ws-a"}, engine, false, false, true},
		{"static token sees everything", &console.Principal{Subject: "static-token", Unscoped: true}, unscoped, true, true, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.principal != nil {
				ctx = console.WithPrincipal(ctx, *test.principal)
			}
			grant, visible, err := access(ctx, test.store)
			if (err != nil) != test.wantErr || visible != test.wantVisible || grant.Payloads != test.wantPayloads {
				t.Fatalf("visible=%t payloads=%t err=%v", visible, grant.Payloads, err)
			}
		})
	}
}

func TestMACPageTokens(t *testing.T) {
	now := clockNow
	tokens, err := console.NewMACPageTokens([]byte(strings.Repeat("k", 32)), time.Hour, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	workspaceA := console.WithPrincipal(context.Background(), console.Principal{Subject: "u", Workspace: "ws-a"})
	workspaceB := console.WithPrincipal(context.Background(), console.Principal{Subject: "u", Workspace: "ws-b"})
	token, err := tokens.Seal(workspaceA, "directory|engine|default", "temporaless/v2/default/_latest/pull:odds.binpb")
	if err != nil {
		t.Fatal(err)
	}
	if cursor, err := tokens.Open(workspaceA, "directory|engine|default", token); err != nil || !strings.HasSuffix(cursor, "pull:odds.binpb") {
		t.Fatalf("open = %q, %v", cursor, err)
	}
	flipped := []byte(token)
	flipped[3] ^= 1
	tests := []struct {
		name    string
		ctx     context.Context
		binding string
		token   string
	}{
		{"other filters", workspaceA, "directory|engine|default|failed", token},
		{"other workspace", workspaceB, "directory|engine|default", token},
		{"tampered", workspaceA, "directory|engine|default", string(flipped)},
		{"malformed", workspaceA, "directory|engine|default", "not-a-token"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := tokens.Open(test.ctx, test.binding, test.token); err == nil {
				t.Fatal("token opened")
			}
		})
	}
	now = now.Add(2 * time.Hour)
	if _, err := tokens.Open(workspaceA, "directory|engine|default", token); err == nil {
		t.Fatal("expired token opened")
	}
	if _, err := console.NewMACPageTokens([]byte("short"), 0, nil); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestStaticToken(t *testing.T) {
	static, err := console.NewStaticTokenFromFile(writeFile(t, "token", []byte("  a-long-static-operator-token \n")))
	if err != nil {
		t.Fatal(err)
	}
	if principal, err := static.Authenticate(context.Background(), "a-long-static-operator-token"); err != nil || !principal.Unscoped {
		t.Fatalf("principal=%+v err=%v", principal, err)
	}
	if _, err := static.Authenticate(context.Background(), "a-long-static-operator-tokeN"); err == nil {
		t.Fatal("wrong token accepted")
	}
	if _, err := console.NewStaticTokenFromFile(writeFile(t, "short", []byte("short"))); err == nil {
		t.Fatal("short static token accepted")
	}
}
