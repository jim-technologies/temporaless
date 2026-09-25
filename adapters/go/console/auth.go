package console

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Principal is the verified caller.
type Principal struct {
	// Subject is the verified subject claim.
	Subject string
	// Workspace is the verified tenant claim. Empty for a static-token or
	// loopback caller, which is not tenant-scoped.
	Workspace string
	// Unscoped marks a static-token or loopback caller that may inspect every
	// registered store.
	Unscoped bool
}

type principalKey struct{}

// WithPrincipal returns a context carrying the verified principal.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, principal)
}

// PrincipalFrom returns the verified principal, if any.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalKey{}).(Principal)
	return principal, ok
}

// Authenticator verifies one bearer credential.
type Authenticator interface {
	Authenticate(ctx context.Context, bearer string) (Principal, error)
}

// ErrUnauthenticated marks a missing or invalid credential.
var ErrUnauthenticated = errors.New("unauthenticated")

// StaticToken authenticates one shared bearer token read from a mounted file.
// It suits a single-operator deployment on a private network; hosted
// multi-tenant deployments use JWTVerifier.
type StaticToken struct {
	token []byte
}

// NewStaticTokenFromFile reads the token, trimming surrounding whitespace.
func NewStaticTokenFromFile(path string) (*StaticToken, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	token := []byte(strings.TrimSpace(string(data)))
	if len(token) < 16 {
		return nil, fmt.Errorf("static token in %s is shorter than 16 bytes", path)
	}
	return &StaticToken{token: token}, nil
}

// Authenticate implements Authenticator.
func (static *StaticToken) Authenticate(_ context.Context, bearer string) (Principal, error) {
	if subtle.ConstantTimeCompare([]byte(bearer), static.token) != 1 {
		return Principal{}, ErrUnauthenticated
	}
	return Principal{Subject: "static-token", Unscoped: true}, nil
}

// JWTConfig configures JWTVerifier. Claim names are configurable so any
// OIDC-style issuer fits without code changes.
type JWTConfig struct {
	// Issuer must equal the token's iss claim.
	Issuer string
	// Audience must appear in the token's aud claim.
	Audience string
	// SubjectClaim names the principal claim. Empty selects "sub".
	SubjectClaim string
	// WorkspaceClaim names the tenant claim. Empty selects "workspace_id".
	WorkspaceClaim string
	// Leeway tolerates clock skew on exp and nbf. Zero selects 30 seconds.
	Leeway time.Duration
}

// KeySource returns the verification key for a key ID.
type KeySource interface {
	Key(ctx context.Context, keyID string) (crypto.PublicKey, error)
}

// JWTVerifier verifies ES256 and RS256 compact JWTs against a JWKS.
type JWTVerifier struct {
	config JWTConfig
	keys   KeySource
	now    func() time.Time
}

// NewJWTVerifier validates the configuration.
func NewJWTVerifier(config JWTConfig, keys KeySource, now func() time.Time) (*JWTVerifier, error) {
	if config.Issuer == "" || config.Audience == "" {
		return nil, errors.New("jwt: issuer and audience are required")
	}
	if keys == nil {
		return nil, errors.New("jwt: a key source is required")
	}
	if config.SubjectClaim == "" {
		config.SubjectClaim = "sub"
	}
	if config.WorkspaceClaim == "" {
		config.WorkspaceClaim = "workspace_id"
	}
	if config.Leeway == 0 {
		config.Leeway = 30 * time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &JWTVerifier{config: config, keys: keys, now: now}, nil
}

// Authenticate implements Authenticator.
func (verifier *JWTVerifier) Authenticate(ctx context.Context, bearer string) (Principal, error) {
	parts := strings.Split(bearer, ".")
	if len(parts) != 3 {
		return Principal{}, fmt.Errorf("%w: malformed token", ErrUnauthenticated)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := decodeSegment(parts[0], &header); err != nil {
		return Principal{}, fmt.Errorf("%w: header: %w", ErrUnauthenticated, err)
	}
	key, err := verifier.keys.Key(ctx, header.Kid)
	if err != nil {
		return Principal{}, fmt.Errorf("%w: key %q: %w", ErrUnauthenticated, header.Kid, err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return Principal{}, fmt.Errorf("%w: signature encoding", ErrUnauthenticated)
	}
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	// The algorithm must match the key type; alg=none and HMAC are refused.
	switch typed := key.(type) {
	case *ecdsa.PublicKey:
		if header.Alg != "ES256" || typed.Curve != elliptic.P256() || len(signature) != 64 ||
			!ecdsa.Verify(typed, digest[:], new(big.Int).SetBytes(signature[:32]), new(big.Int).SetBytes(signature[32:])) {
			return Principal{}, fmt.Errorf("%w: signature", ErrUnauthenticated)
		}
	case *rsa.PublicKey:
		if header.Alg != "RS256" || rsa.VerifyPKCS1v15(typed, crypto.SHA256, digest[:], signature) != nil {
			return Principal{}, fmt.Errorf("%w: signature", ErrUnauthenticated)
		}
	default:
		return Principal{}, fmt.Errorf("%w: unsupported key type", ErrUnauthenticated)
	}

	claims := map[string]any{}
	if err := decodeSegment(parts[1], &claims); err != nil {
		return Principal{}, fmt.Errorf("%w: claims: %w", ErrUnauthenticated, err)
	}
	now := verifier.now()
	if issuer, _ := claims["iss"].(string); issuer != verifier.config.Issuer {
		return Principal{}, fmt.Errorf("%w: issuer", ErrUnauthenticated)
	}
	if !audienceContains(claims["aud"], verifier.config.Audience) {
		return Principal{}, fmt.Errorf("%w: audience", ErrUnauthenticated)
	}
	expires, ok := numericDate(claims["exp"])
	if !ok || !now.Before(expires.Add(verifier.config.Leeway)) {
		return Principal{}, fmt.Errorf("%w: expired", ErrUnauthenticated)
	}
	if notBefore, ok := numericDate(claims["nbf"]); ok && now.Add(verifier.config.Leeway).Before(notBefore) {
		return Principal{}, fmt.Errorf("%w: not yet valid", ErrUnauthenticated)
	}
	subject, _ := claims[verifier.config.SubjectClaim].(string)
	workspace, _ := claims[verifier.config.WorkspaceClaim].(string)
	if subject == "" || workspace == "" {
		return Principal{}, fmt.Errorf("%w: subject and workspace claims are required", ErrUnauthenticated)
	}
	return Principal{Subject: subject, Workspace: workspace}, nil
}

func decodeSegment(segment string, into any) error {
	data, err := base64.RawURLEncoding.DecodeString(segment)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, into)
}

func audienceContains(value any, audience string) bool {
	switch typed := value.(type) {
	case string:
		return typed == audience
	case []any:
		for _, item := range typed {
			if item == audience {
				return true
			}
		}
	}
	return false
}

func numericDate(value any) (time.Time, bool) {
	seconds, ok := value.(float64)
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(int64(seconds), 0), true
}

// JWKS is a KeySource over a JSON Web Key Set read from a URL or a file. A
// URL is re-fetched at most once per MinRefresh when an unknown key ID
// arrives (key rotation), and otherwise after MaxAge.
type JWKS struct {
	url        string
	file       string
	client     *http.Client
	now        func() time.Time
	minRefresh time.Duration
	maxAge     time.Duration

	mu      sync.Mutex
	keys    map[string]crypto.PublicKey
	fetched time.Time
}

// NewJWKS builds a key source. Exactly one of url and file must be set.
func NewJWKS(url, file string, client *http.Client, now func() time.Time) (*JWKS, error) {
	if (url == "") == (file == "") {
		return nil, errors.New("jwks: set exactly one of url and file")
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	if now == nil {
		now = time.Now
	}
	return &JWKS{url: url, file: file, client: client, now: now, minRefresh: 30 * time.Second, maxAge: 10 * time.Minute}, nil
}

// Key implements KeySource.
func (jwks *JWKS) Key(ctx context.Context, keyID string) (crypto.PublicKey, error) {
	jwks.mu.Lock()
	defer jwks.mu.Unlock()
	now := jwks.now()
	key, known := jwks.keys[keyID]
	stale := jwks.keys == nil || now.Sub(jwks.fetched) > jwks.maxAge
	if known && !stale {
		return key, nil
	}
	if !stale && now.Sub(jwks.fetched) < jwks.minRefresh {
		return nil, errors.New("unknown key id")
	}
	keys, err := jwks.load(ctx)
	if err != nil {
		if known {
			return key, nil
		}
		return nil, err
	}
	jwks.keys, jwks.fetched = keys, now
	if key, ok := keys[keyID]; ok {
		return key, nil
	}
	return nil, errors.New("unknown key id")
}

func (jwks *JWKS) load(ctx context.Context) (map[string]crypto.PublicKey, error) {
	var data []byte
	if jwks.file != "" {
		var err error
		if data, err = os.ReadFile(jwks.file); err != nil {
			return nil, err
		}
	} else {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, jwks.url, nil)
		if err != nil {
			return nil, err
		}
		response, err := jwks.client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("jwks fetch: HTTP %d", response.StatusCode)
		}
		if data, err = io.ReadAll(io.LimitReader(response.Body, 1<<20)); err != nil {
			return nil, err
		}
	}
	return ParseJWKS(data)
}

// ParseJWKS decodes the EC P-256 and RSA signing keys of a JSON Web Key Set.
func ParseJWKS(data []byte) (map[string]crypto.PublicKey, error) {
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Crv string `json:"crv"`
			X   string `json:"x"`
			Y   string `json:"y"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(data, &set); err != nil {
		return nil, fmt.Errorf("jwks: %w", err)
	}
	keys := make(map[string]crypto.PublicKey, len(set.Keys))
	for _, jwk := range set.Keys {
		if jwk.Use != "" && jwk.Use != "sig" {
			continue
		}
		switch jwk.Kty {
		case "EC":
			if jwk.Crv != "P-256" {
				continue
			}
			x, errX := base64.RawURLEncoding.DecodeString(jwk.X)
			y, errY := base64.RawURLEncoding.DecodeString(jwk.Y)
			if errX != nil || errY != nil || len(x) != 32 || len(y) != 32 {
				return nil, fmt.Errorf("jwks: key %q has invalid coordinates", jwk.Kid)
			}
			uncompressed := append(append([]byte{4}, x...), y...)
			key, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), uncompressed)
			if err != nil {
				return nil, fmt.Errorf("jwks: key %q: %w", jwk.Kid, err)
			}
			keys[jwk.Kid] = key
		case "RSA":
			n, errN := base64.RawURLEncoding.DecodeString(jwk.N)
			e, errE := base64.RawURLEncoding.DecodeString(jwk.E)
			if errN != nil || errE != nil || len(e) == 0 || len(e) > 4 {
				return nil, fmt.Errorf("jwks: key %q has invalid parameters", jwk.Kid)
			}
			exponent := 0
			for _, b := range e {
				exponent = exponent<<8 | int(b)
			}
			key := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: exponent}
			if key.N.BitLen() < 2048 {
				return nil, fmt.Errorf("jwks: key %q is shorter than 2048 bits", jwk.Kid)
			}
			keys[jwk.Kid] = key
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("jwks: no usable signing keys")
	}
	return keys, nil
}

// Authorizer answers one relationship check, such as an OpenFGA Check.
type Authorizer interface {
	Check(ctx context.Context, user, relation, object string) (bool, error)
}

// OpenFGA checks relationships over OpenFGA's HTTP API. Decisions are cached
// briefly so a dashboard's burst of source requests costs one check.
type OpenFGA struct {
	apiURL   string
	storeID  string
	modelID  string
	token    string
	client   *http.Client
	now      func() time.Time
	cacheTTL time.Duration

	mu    sync.Mutex
	cache map[string]cachedDecision
}

type cachedDecision struct {
	allowed bool
	expires time.Time
}

// OpenFGAConfig configures OpenFGA.
type OpenFGAConfig struct {
	// APIURL is the OpenFGA HTTP API base URL.
	APIURL string
	// StoreID is the OpenFGA store.
	StoreID string
	// AuthorizationModelID pins a model; empty uses the store's latest.
	AuthorizationModelID string
	// TokenFile holds an optional API bearer token (mounted secret).
	TokenFile string
	// CacheTTL caches decisions; zero selects 30 seconds.
	CacheTTL time.Duration
}

// NewOpenFGA builds the authorizer.
func NewOpenFGA(config OpenFGAConfig, client *http.Client, now func() time.Time) (*OpenFGA, error) {
	if config.APIURL == "" || config.StoreID == "" {
		return nil, errors.New("openfga: api_url and store_id are required")
	}
	token := ""
	if config.TokenFile != "" {
		data, err := os.ReadFile(config.TokenFile)
		if err != nil {
			return nil, err
		}
		token = strings.TrimSpace(string(data))
	}
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	if now == nil {
		now = time.Now
	}
	ttl := config.CacheTTL
	if ttl == 0 {
		ttl = 30 * time.Second
	}
	return &OpenFGA{
		apiURL: strings.TrimRight(config.APIURL, "/"), storeID: config.StoreID, modelID: config.AuthorizationModelID,
		token: token, client: client, now: now, cacheTTL: ttl, cache: map[string]cachedDecision{},
	}, nil
}

// Check implements Authorizer. Any failure to reach OpenFGA denies.
func (fga *OpenFGA) Check(ctx context.Context, user, relation, object string) (bool, error) {
	cacheKey := user + "\x00" + relation + "\x00" + object
	now := fga.now()
	fga.mu.Lock()
	if decision, ok := fga.cache[cacheKey]; ok && now.Before(decision.expires) {
		fga.mu.Unlock()
		return decision.allowed, nil
	}
	fga.mu.Unlock()

	body := map[string]any{"tuple_key": map[string]string{"user": user, "relation": relation, "object": object}}
	if fga.modelID != "" {
		body["authorization_model_id"] = fga.modelID
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return false, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, fga.apiURL+"/stores/"+fga.storeID+"/check", strings.NewReader(string(payload)))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	if fga.token != "" {
		request.Header.Set("Authorization", "Bearer "+fga.token)
	}
	response, err := fga.client.Do(request)
	if err != nil {
		return false, fmt.Errorf("openfga check: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("openfga check: HTTP %d", response.StatusCode)
	}
	var decision struct {
		Allowed bool `json:"allowed"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&decision); err != nil {
		return false, fmt.Errorf("openfga check: %w", err)
	}
	fga.mu.Lock()
	if len(fga.cache) > 10000 {
		fga.cache = map[string]cachedDecision{}
	}
	fga.cache[cacheKey] = cachedDecision{allowed: decision.Allowed, expires: now.Add(fga.cacheTTL)}
	fga.mu.Unlock()
	return decision.Allowed, nil
}

// StoreScope binds one registered store to the tenant that may read it.
type StoreScope struct {
	// Workspace is the tenant (the verified workspace claim) that owns the
	// store. Callers from any other workspace never see it.
	Workspace string
	// ReadRelation is checked on the workspace object to see the store.
	// Empty selects "can_read".
	ReadRelation string
	// PayloadRelation is checked to see payload values. Empty selects the
	// read relation; set "can_write" to show payloads to editors only.
	PayloadRelation string
}

// ScopedAccessConfig names the relationship objects the checks use.
type ScopedAccessConfig struct {
	// Scopes maps store ID to its tenant binding. A store without a scope is
	// visible only to unscoped (static-token or loopback) callers.
	Scopes map[string]StoreScope
	// UserType prefixes the subject, e.g. "user" → "user:<sub>".
	UserType string
	// WorkspaceType prefixes the workspace, e.g. "workspace".
	WorkspaceType string
}

// ScopedAccess returns an inspection.Access that forces scope from the
// verified principal: a store is visible only to its own workspace and only
// when the authorizer allows the read relation; payloads need the payload
// relation. Unscoped principals see everything; a missing principal sees
// nothing.
func ScopedAccess(config ScopedAccessConfig, authorizer Authorizer) inspection.Access {
	userType := defaultString(config.UserType, "user")
	workspaceType := defaultString(config.WorkspaceType, "workspace")
	return func(ctx context.Context, store *inspection.Store) (inspection.Grant, bool, error) {
		principal, ok := PrincipalFrom(ctx)
		if !ok {
			return inspection.Grant{}, false, nil
		}
		if principal.Unscoped {
			return inspection.Grant{Payloads: true}, true, nil
		}
		scope, ok := config.Scopes[store.ID]
		if !ok || scope.Workspace == "" || scope.Workspace != principal.Workspace || authorizer == nil {
			return inspection.Grant{}, false, nil
		}
		user := userType + ":" + principal.Subject
		object := workspaceType + ":" + principal.Workspace
		readRelation := defaultString(scope.ReadRelation, "can_read")
		allowed, err := authorizer.Check(ctx, user, readRelation, object)
		if err != nil {
			return inspection.Grant{}, false, status.Error(codes.Unavailable, "authorization is unavailable")
		}
		if !allowed {
			return inspection.Grant{}, false, nil
		}
		payloadRelation := defaultString(scope.PayloadRelation, readRelation)
		payloads := true
		if payloadRelation != readRelation {
			if payloads, err = authorizer.Check(ctx, user, payloadRelation, object); err != nil {
				return inspection.Grant{}, false, status.Error(codes.Unavailable, "authorization is unavailable")
			}
		}
		return inspection.Grant{Payloads: payloads}, true, nil
	}
}

// MACPageTokens seals page cursors with HMAC-SHA256 over the request binding,
// the caller's tenant, an expiry, and the cursor, so a token cannot be
// replayed under another store, namespace, filter, or workspace.
type MACPageTokens struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// NewMACPageTokensFromFile reads a key of at least 32 bytes from a mounted
// secret file.
func NewMACPageTokensFromFile(path string, ttl time.Duration, now func() time.Time) (*MACPageTokens, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return NewMACPageTokens([]byte(strings.TrimSpace(string(data))), ttl, now)
}

// NewMACPageTokens builds the sealer. ttl zero selects one hour.
func NewMACPageTokens(key []byte, ttl time.Duration, now func() time.Time) (*MACPageTokens, error) {
	if len(key) < 32 {
		return nil, errors.New("page-token key must be at least 32 bytes")
	}
	if ttl == 0 {
		ttl = time.Hour
	}
	if now == nil {
		now = time.Now
	}
	return &MACPageTokens{key: slices.Clone(key), ttl: ttl, now: now}, nil
}

// Seal implements inspection.PageTokens.
func (tokens *MACPageTokens) Seal(ctx context.Context, binding string, cursor string) (string, error) {
	expires := make([]byte, 8)
	binary.BigEndian.PutUint64(expires, uint64(tokens.now().Add(tokens.ttl).Unix()))
	body := append(expires, cursor...)
	return base64.RawURLEncoding.EncodeToString(body) + "." +
		base64.RawURLEncoding.EncodeToString(tokens.mac(ctx, binding, body)), nil
}

// Open implements inspection.PageTokens.
func (tokens *MACPageTokens) Open(ctx context.Context, binding string, token string) (string, error) {
	encodedBody, encodedMAC, ok := strings.Cut(token, ".")
	if !ok {
		return "", errors.New("malformed page token")
	}
	body, errBody := base64.RawURLEncoding.DecodeString(encodedBody)
	mac, errMAC := base64.RawURLEncoding.DecodeString(encodedMAC)
	if errBody != nil || errMAC != nil || len(body) < 8 {
		return "", errors.New("malformed page token")
	}
	if !hmac.Equal(mac, tokens.mac(ctx, binding, body)) {
		return "", errors.New("page token does not belong to this request")
	}
	if tokens.now().Unix() > int64(binary.BigEndian.Uint64(body[:8])) {
		return "", errors.New("page token expired")
	}
	return string(body[8:]), nil
}

func (tokens *MACPageTokens) mac(ctx context.Context, binding string, body []byte) []byte {
	principal, _ := PrincipalFrom(ctx)
	mac := hmac.New(sha256.New, tokens.key)
	for _, part := range [][]byte{[]byte("temporaless-console/page-token/v1"), []byte(binding), []byte(principal.Workspace), body} {
		length := make([]byte, 4)
		binary.BigEndian.PutUint32(length, uint32(len(part)))
		mac.Write(length)
		mac.Write(part)
	}
	return mac.Sum(nil)
}

// RequireBearer wraps an HTTP handler: it authenticates the Authorization
// bearer and places the principal in the request context, or answers 401.
func RequireBearer(authenticator Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearer, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			unauthorized(w)
			return
		}
		principal, err := authenticator.Authenticate(r.Context(), bearer)
		if err != nil {
			unauthorized(w)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}

// LoopbackPrincipal marks every request as the unscoped loopback operator. Use
// it only behind a listener bound to a loopback address.
func LoopbackPrincipal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), Principal{Subject: "loopback", Unscoped: true})))
	})
}

func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="temporaless-console"`)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_, _ = w.Write([]byte(`{"code":"unauthenticated","message":"a valid bearer token is required"}`))
}

func bearerToken(header string) (string, bool) {
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
		return "", false
	}
	return strings.TrimSpace(token), true
}

// UnaryAuthentication authenticates native gRPC calls from their
// authorization metadata. Calls that already carry a principal (the HTTP
// projection, authenticated by RequireBearer) pass through.
func UnaryAuthentication(authenticator Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if _, ok := PrincipalFrom(ctx); ok {
			return handler(ctx, request)
		}
		if authenticator == nil {
			return nil, status.Error(codes.Unauthenticated, "a valid bearer token is required")
		}
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 1 {
			return nil, status.Error(codes.Unauthenticated, "a valid bearer token is required")
		}
		bearer, ok := bearerToken(values[0])
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "a valid bearer token is required")
		}
		principal, err := authenticator.Authenticate(ctx, bearer)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, "a valid bearer token is required")
		}
		return handler(WithPrincipal(ctx, principal), request)
	}
}
