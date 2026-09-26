package console

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
	"time"

	invariant "github.com/jim-technologies/invariantprotocol/go"
	"github.com/jim-technologies/temporaless/adapters/go/console/internal/terminalv1"
	inspectionv1 "github.com/jim-technologies/temporaless/core/go/gen/temporaless/v1/inspectionv1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

//go:embed descriptors/*.binpb
var descriptors embed.FS

//go:embed templates/executions.json
var executionsTemplate []byte

// ExecutionsTemplate returns the dashboard template the console UI renders.
// Hosts that embed the console's sources elsewhere load the same template.
func ExecutionsTemplate() []byte {
	return bytes.Clone(executionsTemplate)
}

// DescriptorSet returns one FileDescriptorSet, with source comments, holding
// both registered contracts: RunInspectionService and the terminal contract.
func DescriptorSet() ([]byte, error) {
	merged := &descriptorpb.FileDescriptorSet{}
	seen := map[string]bool{}
	for _, name := range []string{"descriptors/temporaless.binpb", "descriptors/terminal.binpb"} {
		data, err := descriptors.ReadFile(name)
		if err != nil {
			return nil, err
		}
		set := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(data, set); err != nil {
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
		for _, file := range set.GetFile() {
			if !seen[file.GetName()] {
				seen[file.GetName()] = true
				merged.File = append(merged.File, file)
			}
		}
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(merged)
}

// ProjectedMethods lists the only methods the HTTP, MCP, and CLI projections
// expose. Everything else on the terminal contract (streams, AI, actions) is
// absent there and answers Unimplemented over native gRPC.
func ProjectedMethods() []string {
	terminal := terminalv1.TerminalService_ServiceDesc.ServiceName
	return []string{
		inspectionv1.RunInspectionService_ServiceDesc.ServiceName + ".*",
		terminal + ".Get",
		terminal + ".ListSources",
	}
}

// NewInvariantServer registers the read-only services on one Invariant
// Protocol server. authenticator authenticates native gRPC calls; HTTP calls
// are authenticated by Handler before they reach the projection.
func NewInvariantServer(inspection inspectionv1.RunInspectionServiceServer, terminal *TerminalService, authenticator Authenticator) (*invariant.Server, error) {
	descriptor, err := DescriptorSet()
	if err != nil {
		return nil, err
	}
	server, err := invariant.ServerFromBytes(descriptor)
	if err != nil {
		return nil, err
	}
	server.Include(ProjectedMethods()...)
	server.SetMaxUnaryRequestBytes(64 << 10)
	server.Use(UnaryAuthentication(authenticator))
	validation, err := invariant.Validation()
	if err != nil {
		return nil, err
	}
	server.Use(validation)
	inspectionv1.RegisterRunInspectionServiceServer(server, inspection)
	terminalv1.RegisterTerminalServiceServer(server, terminal)
	return server, nil
}

// UIConfig is the browser configuration served at /ui/config. It carries no
// secret.
type UIConfig struct {
	Title    string          `json:"title"`
	Auth     string          `json:"auth"`
	Template json.RawMessage `json:"template"`
}

// HandlerOptions assembles the console's HTTP surface.
type HandlerOptions struct {
	// API is the Invariant Protocol HTTP projection.
	API http.Handler
	// Authenticator guards the API. Nil means loopback mode: every request is
	// the unscoped loopback operator, so bind only to a loopback address.
	Authenticator Authenticator
	// Assets holds the built UI (index.html at its root). Nil serves a short
	// page explaining how to build it.
	Assets fs.FS
	// Title names the console in the UI.
	Title string
}

// Handler returns the console's HTTP handler: the authenticated API, the UI,
// its configuration, and probes. Reads only; no route accepts a mutation.
func Handler(options HandlerOptions) (http.Handler, error) {
	if options.API == nil {
		return nil, errors.New("console: API handler is required")
	}
	api := LoopbackPrincipal(options.API)
	authMode := "loopback"
	if options.Authenticator != nil {
		api = RequireBearer(options.Authenticator, options.API)
		authMode = "bearer"
	}
	config, err := json.Marshal(UIConfig{Title: defaultString(options.Title, "Temporaless console"), Auth: authMode, Template: executionsTemplate})
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /ui/config", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(config)
	})
	// Every POST is an API call: Connect/HTTP methods and MCP (POST /mcp).
	mux.Handle("POST /", noStore(api))
	mux.Handle("GET /__invariant/", noStore(api))
	mux.Handle("GET /", uiHandler(options.Assets))
	return securityHeaders(mux), nil
}

func noStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := w.Header()
		header.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; "+
			"font-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		header.Set("X-Content-Type-Options", "nosniff")
		header.Set("Referrer-Policy", "no-referrer")
		header.Set("Cross-Origin-Opener-Policy", "same-origin")
		next.ServeHTTP(w, r)
	})
}

const missingUI = `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Temporaless console</title></head>
<body style="font-family:system-ui,sans-serif;max-width:40rem;margin:4rem auto;line-height:1.5">
<h1>Temporaless console</h1><p>This binary was built without its UI. Build it with <code>make build-console</code>;
the read-only API and <code>/ui/config</code> are served either way.</p></body></html>`

// uiHandler serves the built single-page app: real files as themselves,
// every other GET path as index.html so client routes deep-link.
func uiHandler(assets fs.FS) http.Handler {
	if assets == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(missingUI))
		})
	}
	files := http.FileServerFS(assets)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name != "" {
			if info, err := fs.Stat(assets, name); err == nil && !info.IsDir() {
				if strings.HasPrefix(name, "assets/") {
					w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
				}
				files.ServeHTTP(w, r)
				return
			}
		}
		index, err := fs.ReadFile(assets, "index.html")
		if err != nil {
			http.Error(w, "UI not built", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeContent(w, r, "index.html", time.Time{}, bytes.NewReader(index))
	})
}
