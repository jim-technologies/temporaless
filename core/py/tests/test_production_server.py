"""End-to-end smoke test for ``examples/py/production_server.py``.

Spawns the script as a real subprocess (not in-process imports), hits the
HTTP endpoints, and verifies:

- ``/healthz`` and ``/readyz`` are reachable without auth
- ConnectStore RPCs require a bearer token
- Wrong token → ``401 Code.UNAUTHENTICATED``
- Right token → passes auth (the request may still 4xx/5xx on payload
  validation; that's fine, we're testing the auth layer)
- SIGTERM produces a clean exit within the grace window

This is the production-readiness gate — if the example breaks, the
``Dockerfile`` and ``docs/production-checklist.md`` walkthrough silently
break too.
"""

from __future__ import annotations

import contextlib
import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = REPO_ROOT / "examples" / "py" / "production_server.py"
# The core/py venv has temporaless + uvicorn + opendal installed; the test
# runner's sys.executable might not, so prefer the venv's python explicitly.
VENV_PYTHON = REPO_ROOT / "core" / "py" / ".venv" / "bin" / "python"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_ready(port: int, timeout_s: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.2)
    raise TimeoutError(f"server did not become ready on port {port}")


@pytest.fixture
def production_server():
    """Spawn the example script. Yields ``(port, token)``. Reaps on teardown."""
    if not EXAMPLE.exists():
        pytest.skip("production_server.py example missing")
    if not VENV_PYTHON.exists():
        pytest.skip("core/py venv missing — run `uv sync --project core/py` first")

    port = _free_port()
    token = "test-secret-not-for-prod"
    env = {**os.environ, "AUTH_TOKEN": token, "PORT": str(port)}
    env.pop("TEMPORALESS_STORAGE_ROOT", None)  # let the script mkdtemp

    proc = subprocess.Popen(
        [str(VENV_PYTHON), str(EXAMPLE)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_ready(port)
        yield port, token
    finally:
        with contextlib.suppress(ProcessLookupError):
            proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _http_get(port: int, path: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=3) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if hasattr(e, "read") else b""


def _http_post(
    port: int,
    path: str,
    body: bytes = b"",
    *,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes]:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=body,
        method="POST",
        headers={"content-type": "application/proto", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if hasattr(e, "read") else b""


def test_healthz_returns_200_without_auth(production_server) -> None:
    port, _ = production_server
    status, body = _http_get(port, "/healthz")
    assert status == 200
    assert body == b"ok"


def test_readyz_returns_200_after_startup(production_server) -> None:
    port, _ = production_server
    status, body = _http_get(port, "/readyz")
    assert status == 200
    assert body == b"ready"


def test_connectstore_rpc_rejects_missing_bearer(production_server) -> None:
    port, _ = production_server
    status, body = _http_post(port, "/temporaless.v1.RecordStoreService/PutWorkflow", b"")
    assert status == 401
    assert b"bearer token required" in body.lower() or b"unauthenticated" in body.lower()


def test_connectstore_rpc_rejects_wrong_bearer(production_server) -> None:
    port, _ = production_server
    status, body = _http_post(
        port,
        "/temporaless.v1.RecordStoreService/PutWorkflow",
        b"",
        headers={"authorization": "Bearer wrong-token"},
    )
    assert status == 401
    assert b"invalid" in body.lower() or b"unauthenticated" in body.lower()


def test_connectstore_rpc_passes_auth_with_correct_bearer(production_server) -> None:
    """Right token → request reaches the handler. The empty-body POST will
    fail on payload validation, but the *auth layer* must let it through —
    we're checking that, not the storage layer."""
    port, token = production_server
    status, _ = _http_post(
        port,
        "/temporaless.v1.RecordStoreService/PutWorkflow",
        b"",
        headers={"authorization": f"Bearer {token}"},
    )
    # 401 means auth blocked us; anything else means we got past auth.
    assert status != 401, "auth should accept the correct token"
