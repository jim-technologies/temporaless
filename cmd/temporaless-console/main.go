// Command temporaless-console is the optional read-only operator console for
// Temporaless record stores. It serves RunInspectionService and a dashboard
// facade over Connect/HTTP, MCP, and (optionally) native gRPC, plus a small
// embedded UI. It holds no state and writes nothing, so it can scale to zero
// and run with read-only bucket credentials.
//
//	temporaless-console -config console.yaml
//	temporaless-console -config console.yaml -check
package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	// Viewers pick the zone the console displays times in; embedding the
	// zone database resolves every IANA name the same way on any base image.
	_ "time/tzdata"
)

//go:embed all:assets
var embedded embed.FS

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stderr, releaseEmbeddedLibraries))
}

// run parses flags and serves until a signal. release is called once every
// store is open; main passes releaseEmbeddedLibraries.
func run(ctx context.Context, args []string, stderr io.Writer, release func(*slog.Logger)) int {
	flags := flag.NewFlagSet("temporaless-console", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the console configuration (YAML or JSON)")
	check := flags.Bool("check", false, "validate the configuration, open every store, and exit")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	if *configPath == "" {
		fmt.Fprintln(stderr, "temporaless-console: -config is required")
		return 2
	}
	config, err := loadConfig(*configPath)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return 2
	}
	console, err := build(config, uiAssets(), logger)
	if err != nil {
		logger.Error("console did not start", "error", err)
		return 1
	}
	defer console.close()
	// Every store's operator exists now, so the service libraries OpenDAL
	// unpacked into TMPDIR are mapped and their files can go.
	release(logger)
	if *check {
		logger.Info("configuration is valid", "stores", len(config.GetStores()))
		return 0
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", config.GetListen())
	if err != nil {
		logger.Error("listen", "address", config.GetListen(), "error", err)
		return 1
	}
	server := &http.Server{
		Handler:           accessLog(logger, console.handler),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    64 << 10,
	}
	errs := make(chan error, 2)
	go func() { errs <- server.Serve(listener) }()
	if address := config.GetGrpcListen(); address != "" {
		grpcListener, err := net.Listen("tcp", address)
		if err != nil {
			logger.Error("listen", "address", address, "error", err)
			return 1
		}
		go func() { errs <- console.invariant.Serve(grpcListener) }()
		logger.Info("serving native gRPC", "address", grpcListener.Addr().String())
	}
	logger.Info("serving", "address", listener.Addr().String(), "authentication", console.authMode, "stores", len(config.GetStores()))

	select {
	case <-ctx.Done():
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server stopped", "error", err)
			return 1
		}
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	console.invariant.GracefulStop()
	if err := server.Shutdown(shutdown); err != nil {
		logger.Error("shutdown", "error", err)
		return 1
	}
	return 0
}

// uiAssets returns the built UI when the binary was built with it.
func uiAssets() fs.FS {
	dist, err := fs.Sub(embedded, "assets/dist")
	if err != nil {
		return nil
	}
	if _, err := fs.Stat(dist, "index.html"); err != nil {
		return nil
	}
	return dist
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(status int) {
	recorder.status = status
	recorder.ResponseWriter.WriteHeader(status)
}

// accessLog records one structured line per API request. Static assets and
// probes are not logged; nothing about the request body or credentials is.
func accessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		logger.Info("request", "method", r.URL.Path, "status", recorder.status,
			"duration_ms", time.Since(started).Milliseconds(), "request_id", r.Header.Get("X-Request-Id"))
	})
}
