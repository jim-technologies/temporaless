// Production ConnectStore server wiring example (Go parity with
// examples/py/production_server.py).
//
// Demonstrates the storage-service boundary as one runnable binary:
//
//  1. ConnectStore exposed over ConnectRPC for cross-process / cross-region
//     access. This example intentionally compiles only the filesystem OpenDAL
//     service and requires an explicit unsafe acknowledgement.
//  2. Outer bearer-token auth — rejects unauthenticated requests before
//     ConnectRPC reads or decodes their bodies.
//  3. Structured JSON logging via log/slog with per-request correlation IDs
//     threaded via context.Context.
//  4. HTTP health endpoints (/healthz liveness, /readyz readiness) for
//     Kubernetes / load-balancer probes. Probes do not require auth.
//  5. Graceful shutdown on SIGTERM — readyz flips to 503 first, drains
//     in-flight RPCs, then exits.
//
// Run:
//
//	AUTH_TOKEN=secret123 TEMPORALESS_STORAGE_ROOT=/var/lib/temporaless \
//	  TEMPORALESS_ALLOW_UNSAFE_FS=1 go run ./examples/go/production-server/
//	curl http://localhost:8080/healthz
//	curl -X POST -H 'authorization: Bearer secret123' \
//	     http://localhost:8080/temporaless.v1.RecordStoreService/GetStoreCapabilities
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/opendal-go-services/fs"
	opendal "github.com/apache/opendal/bindings/go"
	"github.com/google/uuid"
	"github.com/jim-technologies/temporaless/adapters/go/connectstore"
	"github.com/jim-technologies/temporaless/core/go/storage"
)

type ctxKey int

const correlationIDKey ctxKey = 1

const (
	maxConnectMessageBytes = 8 << 20 // 8 MiB decoded protobuf message.
	maxHTTPRequestBytes    = 8 << 20 // 8 MiB total encoded HTTP request body.
	serverReadTimeout      = 15 * time.Second
	serverWriteTimeout     = 30 * time.Second
	serverIdleTimeout      = 60 * time.Second
	serverHeaderTimeout    = 5 * time.Second
)

// bearerTokenAuth rejects unauthenticated HTTP requests before ConnectRPC reads
// or decodes their bodies.
type bearerTokenAuth struct {
	token  string
	logger *slog.Logger
}

func (a *bearerTokenAuth) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		correlationID := req.Header.Get("x-correlation-id")
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		ctx := context.WithValue(req.Context(), correlationIDKey, correlationID)

		authz := req.Header.Get("authorization")
		if !strings.HasPrefix(authz, "Bearer ") {
			a.logger.WarnContext(ctx, "auth.missing_bearer_prefix",
				slog.String("correlation_id", correlationID))
			http.Error(w, "bearer token required", http.StatusUnauthorized)
			return
		}
		provided := authz[len("Bearer "):]
		if subtle.ConstantTimeCompare([]byte(provided), []byte(a.token)) != 1 {
			a.logger.WarnContext(ctx, "auth.token_mismatch",
				slog.String("correlation_id", correlationID))
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, req.WithContext(ctx))
	})
}

// rpcLogger records authenticated unary RPC outcomes with a correlation ID.
type rpcLogger struct {
	logger *slog.Logger
}

func (l *rpcLogger) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		correlationID, _ := ctx.Value(correlationIDKey).(string)
		if correlationID == "" {
			correlationID = req.Header().Get("x-correlation-id")
			if correlationID == "" {
				correlationID = uuid.NewString()
			}
		}
		ctx = context.WithValue(ctx, correlationIDKey, correlationID)
		start := time.Now()

		resp, err := next(ctx, req)
		elapsed := time.Since(start)
		fields := []any{
			slog.String("correlation_id", correlationID),
			slog.String("procedure", req.Spec().Procedure),
			slog.Duration("elapsed", elapsed),
		}
		if err == nil {
			l.logger.InfoContext(ctx, "rpc.ok", fields...)
			return resp, nil
		}
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			l.logger.WarnContext(ctx, "rpc.connect_error",
				append(fields, slog.String("code", connectErr.Code().String()))...)
		} else {
			l.logger.ErrorContext(ctx, "rpc.unhandled",
				append(fields, slog.String("err", err.Error()))...)
		}
		return resp, err
	}
}

func (l *rpcLogger) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (l *rpcLogger) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	token := os.Getenv("AUTH_TOKEN")
	if token == "" {
		return errors.New("AUTH_TOKEN is required")
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	root := os.Getenv("TEMPORALESS_STORAGE_ROOT")
	if root == "" {
		return errors.New("TEMPORALESS_STORAGE_ROOT is required")
	}
	if os.Getenv("TEMPORALESS_ALLOW_UNSAFE_FS") != "1" {
		return errors.New("filesystem storage is single-node only; set TEMPORALESS_ALLOW_UNSAFE_FS=1 to acknowledge")
	}
	logger.Info("storage.init", slog.String("root", root))

	operator, err := opendal.NewOperator(fs.Scheme, opendal.OperatorOptions{"root": root})
	if err != nil {
		return fmt.Errorf("opendal operator: %w", err)
	}
	defer operator.Close()
	store := storage.NewOpenDALStore(operator)

	auth := &bearerTokenAuth{token: token, logger: logger}
	rpcLog := &rpcLogger{logger: logger}
	path, connectHandler := connectstore.NewHTTPHandler(
		store,
		connect.WithInterceptors(rpcLog),
		connect.WithReadMaxBytes(maxConnectMessageBytes),
	)

	var ready atomic.Bool

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "text/plain; charset=utf-8")
		if ready.Load() {
			_, _ = w.Write([]byte("ready"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("starting"))
	})
	mux.Handle(path, auth.Wrap(connectHandler))

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           http.MaxBytesHandler(mux, maxHTTPRequestBytes),
		ReadTimeout:       serverReadTimeout,
		ReadHeaderTimeout: serverHeaderTimeout,
		WriteTimeout:      serverWriteTimeout,
		IdleTimeout:       serverIdleTimeout,
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("server.listening", slog.String("port", port), slog.String("storage_root", root))
		ready.Store(true)
		if listenErr := server.ListenAndServe(); listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
			serverErr <- listenErr
		}
		close(serverErr)
	}()

	select {
	case <-rootCtx.Done():
		logger.Info("shutdown.signal_received")
	case listenErr, ok := <-serverErr:
		if ok && listenErr != nil {
			return fmt.Errorf("listen: %w", listenErr)
		}
		return nil
	}

	// Drain phase: /readyz → 503 first so the load balancer stops sending
	// traffic, then wait the grace period for in-flight RPCs.
	ready.Store(false)
	logger.Info("shutdown.draining")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Error("shutdown.error", slog.String("err", shutdownErr.Error()))
		return fmt.Errorf("shutdown: %w", shutdownErr)
	}
	logger.Info("shutdown.complete")
	return nil
}
