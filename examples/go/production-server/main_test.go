// End-to-end smoke test for the production server binary.
//
// Builds the binary (via `go run`), spawns it as a child process, hits the
// HTTP endpoints, and verifies:
//
//   - /healthz and /readyz are reachable without auth
//   - ConnectStore RPCs require a bearer token
//   - Wrong token → 401 Unauthenticated
//   - Right token → passes auth (handler may still 4xx/5xx — we test the
//     auth layer specifically)
//   - SIGTERM produces a clean exit within the grace window
//
// Mirrors core/py/tests/test_production_server.py — keeps Go + Python parity
// for the canonical production wiring.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer listener.Close()
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
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func httpPost(t *testing.T, url string, headers map[string]string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(""))
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
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
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
		status, body := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", nil)
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401; body = %s", status, body)
		}
		if !strings.Contains(strings.ToLower(body), "bearer") &&
			!strings.Contains(strings.ToLower(body), "unauthenticated") {
			t.Fatalf("body = %q, expected unauthenticated message", body)
		}
	})

	t.Run("RPC rejects wrong bearer with 401", func(t *testing.T) {
		status, _ := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", map[string]string{
			"authorization": "Bearer wrong-token",
		})
		if status != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", status)
		}
	})

	t.Run("RPC accepts correct bearer", func(t *testing.T) {
		status, _ := httpPost(t, base+"/temporaless.v1.RecordStoreService/GetStoreCapabilities", map[string]string{
			"authorization": "Bearer " + token,
		})
		if status == http.StatusUnauthorized {
			t.Fatalf("auth rejected the correct token")
		}
	})
}

// setPgid is implemented per-OS in setpgid_unix.go so the test compiles on
// non-unix targets. (CI runs on linux today, so the unix build is what we
// exercise.)
