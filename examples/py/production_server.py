"""Production ConnectStore server wiring example.

Demonstrates the storage-service boundary as one runnable script:

1. ``ConnectStore`` exposed over ConnectRPC for cross-process / cross-region
   access. The OpenDAL scheme and options are explicit environment inputs.
2. **Outer bearer-token and body-limit middleware** — rejects unauthenticated
   requests before consuming their bodies and caps encoded request bytes before
   ConnectRPC buffers or decompresses them.
3. **Structured JSON logging with correlation IDs** — one-line-per-request,
   parseable by Loki / Datadog / CloudWatch without a sidecar.
4. **HTTP health endpoints** (``/healthz`` liveness, ``/readyz`` readiness)
   for Kubernetes / load-balancer probes. The probes do not require auth.
5. **Graceful shutdown on SIGTERM** — marks the service unready, stops
   accepting new requests, and gives the ASGI server time to drain.

This process serves durable records only. It intentionally does not pretend to
route timers or cron schedules: application workflow services must wire their
own ``BackgroundWorkers`` handlers, or send due-timer entries to an external
scheduler/queue.

Run::

    AUTH_TOKEN=secret123 \
      TEMPORALESS_STORAGE_SCHEME=fs \
      TEMPORALESS_STORAGE_OPTIONS='{"root":"/var/lib/temporaless"}' \
      TEMPORALESS_ALLOW_UNSAFE_FS=1 \
      uv run --project core/py python examples/py/production_server.py
    curl -H 'Authorization: Bearer secret123' http://localhost:8080/healthz

The script exits cleanly on Ctrl-C / SIGTERM.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import json
import logging
import os
import signal
import sys
import time
import uuid
from collections.abc import Iterable
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

import opendal
import uvicorn
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext

from temporaless import OpenDALStore, asgi_application

MAX_CONNECT_MESSAGE_BYTES = 8 << 20  # 8 MiB decoded protobuf message.
MAX_HTTP_REQUEST_BYTES = 8 << 20  # 8 MiB total encoded ASGI request body.

# ---- structured JSON logging -----------------------------------------------

_correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")


class _JSONFormatter(logging.Formatter):
    """One JSON object per log line. Includes correlation_id when set by the
    outer request guard or RPC logger."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "correlation_id": _correlation_id_var.get(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key in payload or key.startswith("_"):
                continue
            if key in {
                "args",
                "msg",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "name",
                "taskName",
            }:
                continue
            payload[key] = value
        return json.dumps(payload)


def _configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JSONFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.INFO)


_log = logging.getLogger("temporaless.production")


# ---- RPC logging interceptor + outer request guard -------------------------


class RPCLogger:
    """Log authenticated per-RPC outcomes with a correlation ID.

    Implements the connectrpc-python ``MetadataInterceptor`` Protocol —
    structurally typed via ``on_start`` / ``on_end``. The framework's
    ``Interceptor`` symbol is a Union of all interceptor Protocols; to
    implement one you write a regular class with the matching methods,
    *not* a subclass of ``Interceptor`` (it's not a base class).

    Authentication happens in ``_RPCRequestGuard`` outside ConnectRPC so an
    attacker cannot force the runtime to buffer a request body before auth.
    """

    async def on_start(self, ctx: RequestContext) -> dict[str, Any]:
        correlation_id = ctx.request_headers.get("x-correlation-id") or uuid.uuid4().hex
        token_ctx = _correlation_id_var.set(correlation_id)
        return {
            "start": time.perf_counter(),
            "token": token_ctx,
            "correlation_id": correlation_id,
        }

    async def on_end(
        self,
        token: dict[str, Any],
        ctx: RequestContext,
        error: Exception | None,
    ) -> None:
        elapsed_ms = round((time.perf_counter() - token["start"]) * 1000, 2)
        procedure = f"/{ctx.method.service_name}/{ctx.method.name}"
        if error is None:
            _log.info(
                "rpc.ok",
                extra={"elapsed_ms": elapsed_ms, "procedure": procedure},
            )
        elif isinstance(error, ConnectError):
            _log.warning(
                "rpc.connect_error",
                extra={
                    "code": error.code.name
                    if hasattr(error.code, "name")
                    else str(error.code),
                    "elapsed_ms": elapsed_ms,
                    "procedure": procedure,
                },
            )
        else:
            _log.error(
                "rpc.unhandled",
                extra={
                    "elapsed_ms": elapsed_ms,
                    "exc_type": type(error).__name__,
                    "procedure": procedure,
                },
            )
        _correlation_id_var.reset(token["token"])


class _RPCRequestGuard:
    """Authenticate and bound encoded request bytes before invoking ConnectRPC."""

    def __init__(self, app: Any, *, token: str, max_body_bytes: int) -> None:
        if not token:
            raise ValueError("auth token must be non-empty")
        if max_body_bytes <= 0:
            raise ValueError("max_body_bytes must be positive")
        self._app = app
        self._authorization = b"Bearer " + token.encode()
        self._max_body_bytes = max_body_bytes

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") != "http":
            await self._app(scope, receive, send)
            return

        headers = scope.get("headers", ())
        authorization_values = [
            value for name, value in headers if name.lower() == b"authorization"
        ]
        correlation_values = [
            value for name, value in headers if name.lower() == b"x-correlation-id"
        ]
        correlation_id = (
            correlation_values[0].decode("latin-1", errors="replace")
            if len(correlation_values) == 1
            else uuid.uuid4().hex
        )
        token_ctx = _correlation_id_var.set(correlation_id)
        try:
            if len(authorization_values) != 1 or not authorization_values[0].startswith(
                b"Bearer "
            ):
                _log.warning("auth.missing_bearer_prefix")
                await _send_plaintext(send, 401, b"bearer token required")
                return
            if not hmac.compare_digest(authorization_values[0], self._authorization):
                _log.warning("auth.token_mismatch")
                await _send_plaintext(send, 401, b"invalid bearer token")
                return

            content_length_values = [
                value for name, value in headers if name.lower() == b"content-length"
            ]
            if len(content_length_values) > 1:
                await _send_plaintext(send, 400, b"ambiguous content-length")
                return
            if content_length_values:
                raw_content_length = content_length_values[0]
                if not raw_content_length.isdigit():
                    await _send_plaintext(send, 400, b"invalid content-length")
                    return
                content_length = int(raw_content_length)
                if content_length > self._max_body_bytes:
                    _log.warning("request.body_too_large")
                    await _send_plaintext(send, 413, b"request body too large")
                    return

            body = bytearray()
            while True:
                event = await receive()
                if event.get("type") == "http.disconnect":
                    await _send_plaintext(send, 400, b"request body disconnected")
                    return
                if event.get("type") != "http.request":
                    await _send_plaintext(send, 400, b"invalid request body event")
                    return
                chunk = event.get("body", b"")
                if len(body) + len(chunk) > self._max_body_bytes:
                    _log.warning("request.body_too_large")
                    await _send_plaintext(send, 413, b"request body too large")
                    return
                body.extend(chunk)
                if not event.get("more_body", False):
                    break

            replayed = False

            async def replay_receive() -> dict[str, Any]:
                nonlocal replayed
                if replayed:
                    return {"type": "http.disconnect"}
                replayed = True
                return {
                    "type": "http.request",
                    "body": bytes(body),
                    "more_body": False,
                }

            await self._app(scope, replay_receive, send)
        finally:
            _correlation_id_var.reset(token_ctx)


# ---- ASGI app: ConnectStore + health endpoints -----------------------------


def _build_app(
    store: OpenDALStore,
    interceptors: Iterable[Any],
    *,
    ready: asyncio.Event,
    token: str,
):
    """Compose ConnectStore + /healthz + /readyz behind one ASGI callable.

    /healthz: always 200 once the process is alive. K8s liveness — restarts
              the container if this fails.
    /readyz:  200 only after `ready` is set (after the storage service is
              initialized). K8s readiness — stops sending traffic during
              startup / shutdown.
    """
    connect_app = _RPCRequestGuard(
        asgi_application(
            store,
            interceptors=tuple(interceptors),
            read_max_bytes=MAX_CONNECT_MESSAGE_BYTES,
        ),
        token=token,
        max_body_bytes=MAX_HTTP_REQUEST_BYTES,
    )

    async def app(scope: dict, receive, send) -> None:
        if scope["type"] != "http":
            await connect_app(scope, receive, send)
            return

        path = scope.get("path", "")
        if path == "/healthz":
            await _send_plaintext(send, 200, b"ok")
            return
        if path == "/readyz":
            if ready.is_set():
                await _send_plaintext(send, 200, b"ready")
            else:
                await _send_plaintext(send, 503, b"starting")
            return
        await connect_app(scope, receive, send)

    return app


async def _send_plaintext(send, status: int, body: bytes) -> None:
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [(b"content-type", b"text/plain; charset=utf-8")],
        }
    )
    await send({"type": "http.response.body", "body": body})


# ---- entry point -----------------------------------------------------------


async def _main() -> int:
    _configure_logging()

    auth_token = os.environ.get("AUTH_TOKEN")
    if not auth_token:
        _log.error("config.missing", extra={"variable": "AUTH_TOKEN"})
        return 2

    storage_scheme = os.environ.get("TEMPORALESS_STORAGE_SCHEME")
    if not storage_scheme:
        _log.error("config.missing", extra={"variable": "TEMPORALESS_STORAGE_SCHEME"})
        return 2
    try:
        storage_options = json.loads(
            os.environ.get("TEMPORALESS_STORAGE_OPTIONS", "{}")
        )
    except json.JSONDecodeError:
        _log.exception("config.invalid_storage_options")
        return 2
    if not isinstance(storage_options, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in storage_options.items()
    ):
        _log.error(
            "config.invalid_storage_options", extra={"reason": "expected string map"}
        )
        return 2
    normalized_scheme = storage_scheme.casefold()
    if normalized_scheme == "memory":
        _log.error(
            "config.ephemeral_storage_refused",
            extra={
                "hint": "configure durable object storage; memory storage is unsupported"
            },
        )
        return 2
    if (
        normalized_scheme == "fs"
        and os.environ.get("TEMPORALESS_ALLOW_UNSAFE_FS") != "1"
    ):
        _log.error(
            "config.unsafe_local_storage_refused",
            extra={
                "hint": "set TEMPORALESS_ALLOW_UNSAFE_FS=1 only for one-node development"
            },
        )
        return 2

    _log.info("storage.init", extra={"scheme": storage_scheme})
    try:
        operator = opendal.AsyncOperator(storage_scheme, **storage_options)
        store = OpenDALStore(operator)
    except Exception:
        _log.exception("storage.init_failed", extra={"scheme": storage_scheme})
        return 2

    ready = asyncio.Event()
    shutdown = asyncio.Event()
    app = _build_app(store, interceptors=[RPCLogger()], ready=ready, token=auth_token)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",  # noqa: S104  — production server, intentional
        port=int(os.environ.get("PORT", "8080")),
        log_config=None,  # we control logging via _configure_logging
        access_log=False,
        lifespan="off",
        timeout_keep_alive=5,
        timeout_graceful_shutdown=10,
    )
    server = uvicorn.Server(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda s=sig: (
                _log.info("shutdown.signal", extra={"sig": s.name}) or shutdown.set()
            ),
        )

    server_task = asyncio.create_task(server.serve())

    while not server.started:
        if server_task.done():
            with contextlib.suppress(Exception):
                await server_task
            _log.error("server.start_failed")
            return 1
        await asyncio.sleep(0.05)
    ready.set()
    _log.info(
        "server.ready", extra={"port": config.port, "storage_scheme": storage_scheme}
    )

    shutdown_task = asyncio.create_task(shutdown.wait())
    done, _ = await asyncio.wait(
        {shutdown_task, server_task}, return_when=asyncio.FIRST_COMPLETED
    )
    if server_task in done and not shutdown.is_set():
        shutdown_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await shutdown_task
        _log.error("server.stopped_unexpectedly")
        return 1

    _log.info("shutdown.draining")
    ready.clear()  # /readyz starts returning 503 — load balancer stops sending traffic
    server.should_exit = True

    try:
        await asyncio.wait_for(server_task, timeout=10)
    except TimeoutError:
        _log.error("shutdown.timeout")
        server_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await server_task
        return 1
    _log.info("shutdown.complete")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
