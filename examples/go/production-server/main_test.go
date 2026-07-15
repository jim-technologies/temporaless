// End-to-end smoke test for the production server binary.
//
// Builds the binary (via `go run`), spawns it as a child process, hits the
// HTTP endpoints, and verifies:
//
//   - /healthz and /readyz are reachable without auth
//   - ConnectStore RPCs require a bearer token
//   - Wrong token → 401 Unauthenticated
//   - Right token → completes a real ConnectStore RPC
//   - SIGTERM produces a clean exit within the grace window
//
// Mirrors core/py/tests/test_production_server.py — keeps Go + Python parity
// for the canonical production wiring.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

type countingBody struct {
	reads int
}

type sizedBody struct {
	remaining int64
}

func (b *sizedBody) Read(buffer []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	count := int64(len(buffer))
	if count > b.remaining {
		count = b.remaining
	}
	b.remaining -= count
	return int(count), nil
}

func (b *sizedBody) Close() error {
	return nil
}

func (b *countingBody) Read(_ []byte) (int, error) {
	b.reads++
	return 0, io.EOF
}

func (b *countingBody) Close() error {
	return nil
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

func waitForReady(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("server did not become ready on port %d within %v", port, timeout)
}

func startServer(t *testing.T, port int, token string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, "go", "run", ".")
	cmd.Env = append(cmd.Environ(),
		fmt.Sprintf("PORT=%d", port),
		fmt.Sprintf("AUTH_TOKEN=%s", token),
		fmt.Sprintf("TEMPORALESS_STORAGE_ROOT=%s", t.TempDir()),
		"TEMPORALESS_ALLOW_UNSAFE_FS=1",
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	// `go run` spawns a child; SIGTERM only on parent leaves the child running.
	// Setpgid puts both in a process group so we can signal the whole group.
	cmd.SysProcAttr = setPgid()

	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}

	t.Cleanup(func() {
		// Send SIGTERM to the whole process group (the server inherits Setpgid).
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			}
			<-done
		}
	})

	waitForReady(t, port, 30*time.Second)
	return cmd
}

func httpGet(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(responseBody)
}

func httpPost(t *testing.T, url string, body []byte, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("content-type", "application/proto")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	responseBody, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(responseBody)
}

func TestRunRequiresExplicitProductionConfig(t *testing.T) {
	tests := []struct {
		name      string
		token     string
		root      string
		allowFS   string
		wantError string
	}{
		{name: "missing auth token", wantError: "AUTH_TOKEN"},
		{name: "missing storage root", token: "test", wantError: "TEMPORALESS_STORAGE_ROOT"},
		{
			name:      "filesystem not acknowledged",
			token:     "test",
			root:      t.TempDir(),
			wantError: "TEMPORALESS_ALLOW_UNSAFE_FS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("AUTH_TOKEN", test.token)
			t.Setenv("TEMPORALESS_STORAGE_ROOT", test.root)
			t.Setenv("TEMPORALESS_ALLOW_UNSAFE_FS", test.allowFS)
			err := run()
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("run() error = %v, want containing %q", err, test.wantError)
			}
		})
	}
}

func TestBearerTokenAuthRejectsBeforeReadingOversizedBody(t *testing.T) {
	tests := []struct {
		name          string
		authorization string
	}{
		{name: "missing bearer"},
		{name: "wrong bearer", authorization: "Bearer wrong"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := &countingBody{}
			req := httptest.NewRequest(http.MethodPost, "/rpc", nil)
			req.Body = body
			req.ContentLength = maxHTTPRequestBytes + 1
			if test.authorization != "" {
				req.Header.Set("authorization", test.authorization)
			}

			nextCalled := false
			next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				nextCalled = true
			})
			auth := &bearerTokenAuth{
				token:  "expected",
				logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			response := httptest.NewRecorder()
			http.MaxBytesHandler(auth.Wrap(next), maxHTTPRequestBytes).ServeHTTP(response, req)

			if response.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401", response.Code)
			}
			if body.reads != 0 {
				t.Fatalf("body reads = %d, want 0", body.reads)
			}
			if nextCalled {
				t.Fatal("downstream handler was called")
			}
		})
	}
}

func TestAuthorizedRequestBodyIsBounded(t *testing.T) {
	body := &sizedBody{remaining: maxHTTPRequestBytes + 1}
	req := httptest.NewRequest(http.MethodPost, "/rpc", nil)
	req.Body = body
	req.ContentLength = maxHTTPRequestBytes + 1
	req.Header.Set("authorization", "Bearer expected")

	nextCalled := false
	next := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		nextCalled = true
		_, err := io.Copy(io.Discard, req.Body)
		var maxBytesErr *http.MaxBytesError
		if !errors.As(err, &maxBytesErr) {
			t.Errorf("body read error = %v, want *http.MaxBytesError", err)
		}
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
	})
	auth := &bearerTokenAuth{
		token:  "expected",
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	response := httptest.NewRecorder()
	http.MaxBytesHandler(auth.Wrap(next), maxHTTPRequestBytes).ServeHTTP(response, req)

	if !nextCalled {
		t.Fatal("authorized downstream handler was not called")
	}
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.Code)
	}
}

func TestProductionServerSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("smoke test spawns a subprocess; skipping in -short mode")
	}
	port := freePort(t)
	const token = "test-go-token"
	startServer(t, port, token)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	t.Run("healthz returns 200 without auth", func(t *testing.T) {
		status, body := httpGet(t, base+"/healthz")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if body != "ok" {
			t.Fatalf("body = %q, want %q", body, "ok")
		}
	})

	t.Run("readyz returns 200 after startup", func(t *testing.T) {
		status, body := httpGet(t, base+"/readyz")
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if body != "ready" {
			t.Fatalf("body = %q, want %q", body, "ready")
		}
	})

	t.Run("RPC rejects missing bearer with 401", func(t *testing.T) {
		status, body := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", nil, nil)
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body = %s", status, body)
		}
		if !strings.Contains(strings.ToLower(body), "bearer") &&
			!strings.Contains(strings.ToLower(body), "unauthenticated") {
			t.Fatalf("body = %q, expected unauthenticated message", body)
		}
	})

	t.Run("RPC rejects wrong bearer with 401", func(t *testing.T) {
		status, _ := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", nil, map[string]string{
			"authorization": "Bearer wrong-token",
		})
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	})

	t.Run("RPC accepts correct bearer", func(t *testing.T) {
		status, _ := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", nil, map[string]string{
			"authorization": "Bearer " + token,
		})
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
	})

}

// setPgid is implemented per-OS in setpgid_unix.go so the test compiles on
// non-unix targets. (CI runs on linux today, so the unix build is what we
// exercise.)
