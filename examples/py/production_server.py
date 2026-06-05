"""Production-ready Temporaless server example.

Demonstrates the full production wiring as one runnable script:

1. ``ConnectStore`` exposed over ConnectRPC for cross-process / cross-region
   access. Uses local filesystem here; swap the OpenDAL operator scheme to
   ``s3``/``gcs``/``azblob`` for real deployments.
2. **Bearer-token auth interceptor** — rejects unauthenticated requests with
   ``Code.UNAUTHENTICATED`` *before* the handler runs.
3. **Structured JSON logging with correlation IDs** — one-line-per-request,
   parseable by Loki / Datadog / CloudWatch without a sidecar.
4. **HTTP health endpoints** (``/healthz`` liveness, ``/readyz`` readiness)
   for Kubernetes / load-balancer probes. The probes do not require auth.
5. **Graceful shutdown on SIGTERM** — finishes in-flight requests, stops
   accepting new ones, drains the timer-scanner / cron tick loop.
6. **Combined operator-process loop** — periodic timer-scanner +
   cron-scheduler tick alongside the HTTP server in one uvicorn process.
   Split into separate pods if you want independent scaling.

Run::

    AUTH_TOKEN=secret123 uv run --project core/py python examples/py/production_server.py
    curl -H 'Authorization: Bearer secret123' http://localhost:8080/healthz

The script exits cleanly on Ctrl-C / SIGTERM.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
import tempfile
import time
import uuid
from collections.abc import Iterable
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

import opendal
import uvicorn
from connectrpc.code import Code
from connectrpc.errors import ConnectError
from connectrpc.request import RequestContext

from temporaless import OpenDALStore, asgi_application
from temporaless.cronscheduler import Scheduler

# ---- structured JSON logging -----------------------------------------------

_correlation_id_var: ContextVar[str] = ContextVar("correlation_id", default="-")


class _JSONFormatter(logging.Formatter):
    """One JSON object per log line. Includes correlation_id when set by the
    auth interceptor — one of the few places where correlation lives."""

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


# ---- bearer-token auth interceptor -----------------------------------------


class BearerTokenAuth:
    """Reject requests without a valid bearer token + log per-RPC outcomes
    with a correlation ID.

    Implements the connectrpc-python ``MetadataInterceptor`` Protocol —
    structurally typed via ``on_start`` / ``on_end``. The framework's
    ``Interceptor`` symbol is a Union of all interceptor Protocols; to
    implement one you write a regular class with the matching methods,
    *not* a subclass of ``Interceptor`` (it's not a base class).

    The token is read once from the environment. For production, source
    it from your secret manager (Vault / SSM / Secret Manager); never
    hardcode.
    """

    def __init__(self, token: str) -> None:
        if not token:
            raise ValueError("auth token must be non-empty")
        self._token = token

    async def on_start(self, ctx: RequestContext) -> dict[str, Any]:
        correlation_id = (
            ctx.request_headers().get("x-correlation-id") or uuid.uuid4().hex
        )
        token_ctx = _correlation_id_var.set(correlation_id)

        authz = ctx.request_headers().get("authorization", "")
        if not authz.startswith("Bearer "):
            _log.warning("auth.missing_bearer_prefix")
            _correlation_id_var.reset(token_ctx)
            raise ConnectError(Code.UNAUTHENTICATED, "bearer token required")
        if authz[len("Bearer ") :] != self._token:
            _log.warning("auth.token_mismatch")
            _correlation_id_var.reset(token_ctx)
            raise ConnectError(Code.UNAUTHENTICATED, "invalid bearer token")

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
        if error is None:
            _log.info("rpc.ok", extra={"elapsed_ms": elapsed_ms})
        elif isinstance(error, ConnectError):
            _log.warning(
                "rpc.connect_error",
                extra={
                    "code": error.code.name
                    if hasattr(error.code, "name")
                    else str(error.code),
                    "elapsed_ms": elapsed_ms,
                },
            )
        else:
            _log.error(
                "rpc.unhandled",
                extra={"elapsed_ms": elapsed_ms, "exc_type": type(error).__name__},
            )
        _correlation_id_var.reset(token["token"])


# ---- ASGI app: ConnectStore + health endpoints -----------------------------


def _build_app(
    store: OpenDALStore, interceptors: Iterable[Any], *, ready: asyncio.Event
):
    """Compose ConnectStore + /healthz + /readyz behind one ASGI callable.

    /healthz: always 200 once the process is alive. K8s liveness — restarts
              the container if this fails.
    /readyz:  200 only after `ready` is set (after store + scheduler are
              initialized). K8s readiness — stops sending traffic during
              startup / shutdown.
    """
    connect_app = asgi_application(store, interceptors=tuple(interceptors))

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


# ---- operator loop: timer scanner + cron scheduler -------------------------


async def _operator_loop(
    store: OpenDALStore, scheduler: Scheduler, *, shutdown: asyncio.Event
) -> None:
    """Periodic tick that fires due cron schedules and notifies the timer
    scanner. Runs alongside the HTTP server in the same process for the demo;
    in production you might split this into a dedicated pod for scaling.
    """
    _log.info("operator_loop.started")
    try:
        while not shutdown.is_set():
            try:
                fires = await scheduler.tick(datetime.now(UTC))
                if fires:
                    _log.info("scheduler.fired", extra={"count": fires})
            except Exception:
                _log.exception("scheduler.tick_failed")
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=30)
            except TimeoutError:
                pass
    finally:
        _log.info("operator_loop.stopped")


# ---- entry point -----------------------------------------------------------


async def _main() -> int:
    _configure_logging()

    auth_token = os.environ.get("AUTH_TOKEN")
    if not auth_token:
        _log.warning(
            "auth: AUTH_TOKEN not set; defaulting to development value 'dev-token-only'"
        )
        auth_token = "dev-token-only"

    storage_root = os.environ.get(
        "TEMPORALESS_STORAGE_ROOT", tempfile.mkdtemp(prefix="temporaless-prod-")
    )
    _log.info("storage.init", extra={"root": storage_root})

    operator = opendal.AsyncOperator("fs", root=storage_root)
    store = OpenDALStore(operator)

    async def _dispatch(_schedule_id: str, _fire_time: datetime) -> None:
        # Stub: real users dispatch a workflow.run() call here.
        pass

    scheduler = Scheduler(schedules=[], dispatch=_dispatch)

    ready = asyncio.Event()
    shutdown = asyncio.Event()
    auth: Any = BearerTokenAuth(token=auth_token)
    app = _build_app(store, interceptors=[auth], ready=ready)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",  # noqa: S104  — production server, intentional
        port=int(os.environ.get("PORT", "8080")),
        log_config=None,  # we control logging via _configure_logging
        access_log=False,
        lifespan="off",
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

    operator_task = asyncio.create_task(
        _operator_loop(store, scheduler, shutdown=shutdown)
    )
    server_task = asyncio.create_task(server.serve())

    await asyncio.sleep(0.1)
    ready.set()
    _log.info("server.ready", extra={"port": config.port, "storage_root": storage_root})

    await shutdown.wait()
    _log.info("shutdown.draining")
    ready.clear()  # /readyz starts returning 503 — load balancer stops sending traffic
    server.should_exit = True

    with contextlib.suppress(Exception):
        await asyncio.wait_for(server_task, timeout=10)
    operator_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await operator_task
    _log.info("shutdown.complete")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
