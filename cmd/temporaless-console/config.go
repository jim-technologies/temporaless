package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"

	opendalfs "github.com/apache/opendal-go-services/fs"
	opendals3 "github.com/apache/opendal-go-services/s3"
	opendal "github.com/apache/opendal/bindings/go"
	invariant "github.com/jim-technologies/invariantprotocol/go"
	"github.com/jim-technologies/temporaless/adapters/go/console"
	"github.com/jim-technologies/temporaless/adapters/go/console/configv1"
	"github.com/jim-technologies/temporaless/adapters/go/inspection"
	"github.com/jim-technologies/temporaless/core/go/storage"
	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"
)

// loadConfig reads YAML or JSON in ProtoJSON field names. Unknown fields are
// rejected so a typo cannot silently drop a security setting.
func loadConfig(path string) (*configv1.ConsoleConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var document any
	if err := yaml.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	config := &configv1.ConsoleConfig{}
	if err := protojson.Unmarshal(encoded, config); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return config, validateConfig(config)
}

func validateConfig(config *configv1.ConsoleConfig) error {
	if config.GetListen() == "" {
		return errors.New("listen is required")
	}
	if len(config.GetStores()) == 0 {
		return errors.New("at least one store is required")
	}
	switch mode := config.GetAuthentication().GetMode().(type) {
	case *configv1.Authentication_Loopback:
		if !loopbackAddress(config.GetListen()) {
			return fmt.Errorf("loopback authentication needs a loopback listen address, not %q", config.GetListen())
		}
		if address := config.GetGrpcListen(); address != "" {
			return errors.New("loopback authentication cannot serve native gRPC; it has no caller identity there")
		}
	case *configv1.Authentication_StaticToken:
		if mode.StaticToken.GetTokenFile() == "" {
			return errors.New("static_token.token_file is required")
		}
	case *configv1.Authentication_Jwt:
		jwt := mode.Jwt
		if jwt.GetIssuer() == "" || jwt.GetAudience() == "" {
			return errors.New("jwt.issuer and jwt.audience are required")
		}
		if (jwt.GetJwksUrl() == "") == (jwt.GetJwksFile() == "") {
			return errors.New("set exactly one of jwt.jwks_url and jwt.jwks_file")
		}
		if config.GetAuthentication().GetOpenfga().GetApiUrl() == "" || config.GetAuthentication().GetOpenfga().GetStoreId() == "" {
			return errors.New("jwt authentication needs authentication.openfga.api_url and store_id")
		}
		for _, store := range config.GetStores() {
			if store.GetWorkspace() == "" {
				return fmt.Errorf("store %q needs a workspace in jwt mode", store.GetId())
			}
		}
	default:
		return errors.New("authentication must set loopback, static_token, or jwt")
	}
	if config.GetAuthentication().GetLoopback() == nil && config.GetPageTokenKeyFile() == "" {
		return errors.New("page_token_key_file is required unless authentication is loopback")
	}
	seen := map[string]bool{}
	for _, store := range config.GetStores() {
		if store.GetId() == "" || seen[store.GetId()] {
			return fmt.Errorf("store IDs must be present and unique, got %q", store.GetId())
		}
		seen[store.GetId()] = true
		switch backend := store.GetBackend().(type) {
		case *configv1.Store_Filesystem:
			if backend.Filesystem.GetRoot() == "" {
				return fmt.Errorf("store %q: filesystem.root is required", store.GetId())
			}
		case *configv1.Store_S3:
			s3 := backend.S3
			if s3.GetBucket() == "" || s3.GetAccessKeyIdFile() == "" || s3.GetSecretAccessKeyFile() == "" {
				return fmt.Errorf("store %q: s3.bucket and both credential files are required", store.GetId())
			}
		default:
			return fmt.Errorf("store %q: a filesystem or s3 backend is required", store.GetId())
		}
	}
	return nil
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type consoleServer struct {
	handler   http.Handler
	invariant *invariant.Server
	authMode  string
	operators []*opendal.Operator
}

func (server *consoleServer) close() {
	for _, operator := range server.operators {
		operator.Close()
	}
}

// build wires a validated configuration into the console's handler.
func build(config *configv1.ConsoleConfig, assets fs.FS, logger *slog.Logger) (*consoleServer, error) {
	server := &consoleServer{}
	stores := make([]*inspection.Store, 0, len(config.GetStores()))
	scopes := map[string]console.StoreScope{}
	for _, storeConfig := range config.GetStores() {
		operator, err := openOperator(storeConfig)
		if err != nil {
			server.close()
			return nil, fmt.Errorf("store %q: %w", storeConfig.GetId(), err)
		}
		server.operators = append(server.operators, operator)
		store := &inspection.Store{
			ID:           storeConfig.GetId(),
			DisplayName:  storeConfig.GetDisplayName(),
			Namespaces:   storeConfig.GetNamespaces(),
			ListClaims:   storeConfig.GetListClaims(),
			OverdueGrace: storeConfig.GetOverdueGrace().AsDuration(),
			Records:      storage.NewOpenDALStore(operator),
			Bucket:       inspection.NewOpenDALBucket(operator),
		}
		if path := storeConfig.GetPayloadDescriptorsFile(); path != "" {
			descriptors, err := inspection.LoadDescriptorSet(path)
			if err != nil {
				server.close()
				return nil, fmt.Errorf("store %q: %w", storeConfig.GetId(), err)
			}
			if store.Payloads, err = inspection.NewPayloadRenderer(descriptors, 0); err != nil {
				server.close()
				return nil, fmt.Errorf("store %q: %w", storeConfig.GetId(), err)
			}
		}
		stores = append(stores, store)
		scopes[store.ID] = console.StoreScope{
			Workspace:       storeConfig.GetWorkspace(),
			ReadRelation:    storeConfig.GetReadRelation(),
			PayloadRelation: storeConfig.GetPayloadRelation(),
		}
	}

	authentication := config.GetAuthentication()
	var authenticator console.Authenticator
	var authorizer console.Authorizer
	var err error
	switch mode := authentication.GetMode().(type) {
	case *configv1.Authentication_Loopback:
		server.authMode = "loopback"
	case *configv1.Authentication_StaticToken:
		server.authMode = "static-token"
		authenticator, err = console.NewStaticTokenFromFile(mode.StaticToken.GetTokenFile())
	case *configv1.Authentication_Jwt:
		server.authMode = "jwt"
		var keys *console.JWKS
		if keys, err = console.NewJWKS(mode.Jwt.GetJwksUrl(), mode.Jwt.GetJwksFile(), nil, nil); err == nil {
			authenticator, err = console.NewJWTVerifier(console.JWTConfig{
				Issuer:         mode.Jwt.GetIssuer(),
				Audience:       mode.Jwt.GetAudience(),
				SubjectClaim:   mode.Jwt.GetSubjectClaim(),
				WorkspaceClaim: mode.Jwt.GetWorkspaceClaim(),
			}, keys, nil)
		}
		if err == nil {
			fga := authentication.GetOpenfga()
			authorizer, err = console.NewOpenFGA(console.OpenFGAConfig{
				APIURL:               fga.GetApiUrl(),
				StoreID:              fga.GetStoreId(),
				AuthorizationModelID: fga.GetAuthorizationModelId(),
				TokenFile:            fga.GetTokenFile(),
			}, nil, nil)
		}
	}
	if err != nil {
		server.close()
		return nil, err
	}

	var tokens inspection.PageTokens
	if path := config.GetPageTokenKeyFile(); path != "" {
		if tokens, err = console.NewMACPageTokensFromFile(path, 0, nil); err != nil {
			server.close()
			return nil, err
		}
	}
	limits := inspection.DefaultLimits()
	if configured := config.GetLimits(); configured != nil {
		override := func(target *int, value uint32) {
			if value > 0 {
				*target = int(value)
			}
		}
		override(&limits.DefaultPageSize, configured.GetDefaultPageSize())
		override(&limits.MaxPageSize, configured.GetMaxPageSize())
		override(&limits.ReadBudget, configured.GetReadBudget())
		override(&limits.MaxListedRuns, configured.GetMaxListedRuns())
		override(&limits.MaxRunRecords, configured.GetMaxRunRecords())
	}
	access := console.ScopedAccess(console.ScopedAccessConfig{
		Scopes:        scopes,
		UserType:      authentication.GetUserType(),
		WorkspaceType: authentication.GetWorkspaceType(),
	}, authorizer)
	service, err := inspection.NewService(stores, inspection.Options{Access: access, PageTokens: tokens, Limits: limits, Logger: logger})
	if err != nil {
		server.close()
		return nil, err
	}
	terminal := console.NewTerminalService(service, nil)
	if server.invariant, err = console.NewInvariantServer(service, terminal, authenticator); err != nil {
		server.close()
		return nil, err
	}
	server.handler, err = console.Handler(console.HandlerOptions{
		API:           server.invariant.HTTPHandler(),
		Authenticator: authenticator,
		Assets:        assets,
		Title:         config.GetTitle(),
	})
	if err != nil {
		server.close()
		return nil, err
	}
	return server, nil
}

func openOperator(store *configv1.Store) (*opendal.Operator, error) {
	switch backend := store.GetBackend().(type) {
	case *configv1.Store_Filesystem:
		return opendal.NewOperator(opendalfs.Scheme, opendal.OperatorOptions{"root": backend.Filesystem.GetRoot()})
	case *configv1.Store_S3:
		s3 := backend.S3
		accessKeyID, err := readSecret(s3.GetAccessKeyIdFile())
		if err != nil {
			return nil, err
		}
		secretAccessKey, err := readSecret(s3.GetSecretAccessKeyFile())
		if err != nil {
			return nil, err
		}
		options := opendal.OperatorOptions{
			"bucket":            s3.GetBucket(),
			"root":              s3.GetRoot(),
			"access_key_id":     accessKeyID,
			"secret_access_key": secretAccessKey,
			// Credentials come only from the mounted files above, never from
			// ambient environment or profile files.
			"disable_config_load":  "true",
			"disable_ec2_metadata": "true",
		}
		if s3.GetEndpoint() != "" {
			options["endpoint"] = s3.GetEndpoint()
		}
		if s3.GetRegion() != "" {
			options["region"] = s3.GetRegion()
		}
		return opendal.NewOperator(opendals3.Scheme, options)
	default:
		return nil, errors.New("no backend configured")
	}
}

func readSecret(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return value, nil
}

// releaseEmbeddedLibraries removes the native libraries the OpenDAL service
// modules unpacked into TMPDIR. Each process start writes one per scheme and
// the binding never deletes them; once every operator exists they are mapped
// and the files are no longer needed.
func releaseEmbeddedLibraries(logger *slog.Logger) {
	for _, path := range []string{opendalfs.Scheme.Path(), opendals3.Scheme.Path()} {
		if path == "" {
			continue
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			logger.Warn("could not remove unpacked OpenDAL library", "path", path, "error", err)
		}
	}
}
