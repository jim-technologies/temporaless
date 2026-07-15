"""End-to-end smoke test for ``examples/py/production_server.py``.

Spawns the storage-service script as a real subprocess (not in-process
imports), hits the HTTP endpoints, and verifies:

- ``/healthz`` and ``/readyz`` are reachable without auth
- ConnectStore RPCs require a bearer token
- Wrong token → ``401 Code.UNAUTHENTICATED``
- Right token → completes a real ConnectStore RPC
- SIGTERM produces a clean exit within the grace window

This is the production-readiness gate — if the example breaks, the
``Dockerfile`` and ``docs/production-checklist.md`` walkthrough silently
break too.
"""

from __future__ import annotations

import contextlib
import importlib.util
import json
import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import cast

import pytest

from temporaless.v1 import temporaless_pb2

REPO_ROOT = Path(__file__).resolve().parents[3]
EXAMPLE = REPO_ROOT / "examples" / "py" / "production_server.py"
# The core/py venv has temporaless + uvicorn + opendal installed; the test
# runner's sys.executable might not, so prefer the venv's python explicitly.
VENV_PYTHON = REPO_ROOT / "core" / "py" / ".venv" / "bin" / "python"

_EXAMPLE_SPEC = importlib.util.spec_from_file_location(
    "temporaless_production_server_example", EXAMPLE
)
assert _EXAMPLE_SPEC is not None and _EXAMPLE_SPEC.loader is not None
_EXAMPLE_MODULE = importlib.util.module_from_spec(_EXAMPLE_SPEC)
_EXAMPLE_SPEC.loader.exec_module(_EXAMPLE_MODULE)
MAX_HTTP_REQUEST_BYTES = cast(int, vars(_EXAMPLE_MODULE)["MAX_HTTP_REQUEST_BYTES"])
_RPCRequestGuard = vars(_EXAMPLE_MODULE)["_RPCRequestGuard"]


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({}, "AUTH_TOKEN"),
        (
            {
                "AUTH_TOKEN": "test",
                "TEMPORALESS_STORAGE_SCHEME": "fs",
                "TEMPORALESS_STORAGE_OPTIONS": '{"root":"/tmp/temporaless-test"}',
            },
            "unsafe_local_storage_refused",
        ),
        (
            {
                "AUTH_TOKEN": "test",
                "TEMPORALESS_STORAGE_SCHEME": "memory",
                "TEMPORALESS_ALLOW_UNSAFE_FS": "1",
            },
            "ephemeral_storage_refused",
        ),
    ],
)
def test_production_server_fails_closed_without_required_config(
    overrides: dict[str, str], expected: str
) -> None:
    if not VENV_PYTHON.exists():
        pytest.skip("core/py venv missing — run `uv sync --project core/py` first")
    env = dict(os.environ)
    for name in (
        "AUTH_TOKEN",
        "TEMPORALESS_STORAGE_SCHEME",
        "TEMPORALESS_STORAGE_OPTIONS",
        "TEMPORALESS_ALLOW_UNSAFE_FS",
    ):
        env.pop(name, None)
    env.update(overrides)
    result = subprocess.run(
        [str(VENV_PYTHON), str(EXAMPLE)],
        env=env,
        capture_output=True,
        check=False,
        timeout=10,
    )
    output = (result.stdout + result.stderr).decode(errors="replace")
    assert result.returncode == 2
    assert expected in output


def _wait_for_ready(port: int, timeout_s: float = 15.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as resp:
                if resp.status == 200:
                    return
        except urllib.error.URLError, ConnectionError, OSError:
            pass
        time.sleep(0.2)
    raise TimeoutError(f"server did not become ready on port {port}")


@pytest.fixture
def production_server(tmp_path: Path):
    """Spawn the example script. Yields ``(port, token)``. Reaps on teardown."""
    if not EXAMPLE.exists():
        pytest.skip("production_server.py example missing")
    if not VENV_PYTHON.exists():
        pytest.skip("core/py venv missing — run `uv sync --project core/py` first")

    port = _free_port()
    token = "test-secret-not-for-prod"
    env = {
        **os.environ,
        "AUTH_TOKEN": token,
        "PORT": str(port),
        "TEMPORALESS_STORAGE_SCHEME": "fs",
        "TEMPORALESS_STORAGE_OPTIONS": json.dumps({"root": str(tmp_path)}),
        "TEMPORALESS_ALLOW_UNSAFE_FS": "1",
    }

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
        output = proc.stdout.read().decode(errors="replace") if proc.stdout else ""
        assert proc.returncode == 0, output


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
    """Right token reaches storage and returns a valid protobuf response."""
    port, token = production_server
    status, body = _http_post(
        port,
        "/temporaless.v1.RecordStoreService/GetStoreCapabilities",
        temporaless_pb2.GetStoreCapabilitiesRequest().SerializeToString(),
        headers={"authorization": f"Bearer {token}"},
    )
    assert status == 200
    response = temporaless_pb2.GetStoreCapabilitiesResponse.FromString(body)
    assert response.claim_capability == temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS


def test_connectstore_rpc_rejects_oversized_authorized_body(production_server) -> None:
    port, token = production_server
    status, body = _http_post(
        port,
        "/temporaless.v1.RecordStoreService/PutWorkflow",
        b"x",
        headers={
            "authorization": f"Bearer {token}",
            "content-length": str(MAX_HTTP_REQUEST_BYTES + 1),
        },
    )
    assert status == 413
    assert b"too large" in body.lower()


@pytest.mark.asyncio
async def test_outer_guard_rejects_unauthorized_request_without_consuming_body() -> None:
    downstream_called = False
    receive_called = False
    sent: list[dict] = []

    async def downstream(_scope, _receive, _send) -> None:
        nonlocal downstream_called
        downstream_called = True

    async def receive() -> dict:
        nonlocal receive_called
        receive_called = True
        raise AssertionError("unauthorized request body must not be consumed")

    async def send(event: dict) -> None:
        sent.append(event)

    guard = _RPCRequestGuard(downstream, token="expected", max_body_bytes=MAX_HTTP_REQUEST_BYTES)
    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/temporaless.v1.RecordStoreService/PutWorkflow",
            "headers": [(b"content-length", str(MAX_HTTP_REQUEST_BYTES).encode())],
        },
        receive,
        send,
    )

    assert not receive_called
    assert not downstream_called
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("include_content_length", [True, False])
async def test_outer_guard_rejects_oversized_authorized_request(
    include_content_length: bool,
) -> None:
    downstream_called = False
    receive_calls = 0
    sent: list[dict] = []

    async def downstream(_scope, _receive, _send) -> None:
        nonlocal downstream_called
        downstream_called = True

    async def receive() -> dict:
        nonlocal receive_calls
        receive_calls += 1
        return {
            "type": "http.request",
            "body": b"x" * (MAX_HTTP_REQUEST_BYTES + 1),
            "more_body": False,
        }

    async def send(event: dict) -> None:
        sent.append(event)

    headers = [(b"authorization", b"Bearer expected")]
    if include_content_length:
        headers.append((b"content-length", str(MAX_HTTP_REQUEST_BYTES + 1).encode()))
    guard = _RPCRequestGuard(downstream, token="expected", max_body_bytes=MAX_HTTP_REQUEST_BYTES)
    await guard(
        {
            "type": "http",
            "method": "POST",
            "path": "/temporaless.v1.RecordStoreService/PutWorkflow",
            "headers": headers,
        },
        receive,
        send,
    )

    assert receive_calls == (0 if include_content_length else 1)
    assert not downstream_called
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 413
