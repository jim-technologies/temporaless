"""Canonical ConnectRPC-shaped service example.

The framework's design tenet: **a workflow IS a normal gRPC handler**. You
write the standard ConnectRPC method shape ``async def m(self, request, ctx)``
and add ``@wrap_workflow_method`` — replay, idempotency, and persistence
follow without changing the handler interface.

To deploy this over real gRPC/ConnectRPC:

1. Define ``quant_signals.proto`` with your service and message types.
2. ``buf generate`` to produce ``QuantServiceASGIApplication`` and types.
3. Replace ``StringValue`` below with your generated ``FetchRequest`` /
   ``FetchResponse`` types.
4. Mount the service on any ASGI runner::

       import uvicorn
       from your_pkg.gen import QuantServiceASGIApplication

       service = QuantService(store)
       app = QuantServiceASGIApplication(service, interceptors=[auth, rate_limit])
       uvicorn.run(app, host="0.0.0.0", port=8080)

This file uses ``wrapperspb.StringValue`` for the request/response so the
example runs without proto codegen, but the structure is identical to what
you'd write in production.

Run:

    uv run --project core/py python examples/py/quant_service.py
"""

from __future__ import annotations

import asyncio
import random
import tempfile

import opendal
from google.protobuf.wrappers_pb2 import StringValue

from temporaless import (
    ActivityOptions,
    OpenDALStore,
    Options,
    Store,
    annotate,
    current_workflow,
    wrap_workflow_method,
)


async def _vendor_fetch(request: StringValue) -> StringValue:
    """Stand-in for a slow external vendor API. Annotated for observability."""
    annotate("vendor", "alpha")
    annotate("symbol", request.value)
    await asyncio.sleep(random.uniform(0.05, 0.15))
    return StringValue(value=f"{request.value}:100.0+{random.uniform(-5, 5):.2f}")


async def _compose(request: StringValue) -> StringValue:
    annotate("kind", "signal")
    return StringValue(value=f"signal({request.value})")


def _store_of(service: object) -> Store:
    return service._store  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]


def _options_for_fetch_prices(_service: object, request: StringValue) -> Options:
    return Options(
        workflow_id=f"prices:{request.value}",
        run_id="2026-05-04",
        code_version="example",
    )


def _options_for_compose_signal(_service: object, request: StringValue) -> Options:
    return Options(
        workflow_id="signals:batch",
        run_id=request.value,
        code_version="example",
    )


class QuantService:
    """Real ConnectRPC service shape: methods are ``async def m(self, req, ctx)``.

    In production each method below would also appear in ``QuantService``'s
    generated ``Protocol`` (from ``buf generate``); here we just expose them
    as plain methods. The ``@wrap_workflow_method`` decorator is the only
    framework-specific addition — it does NOT change the method signature.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    @wrap_workflow_method(
        store=_store_of,
        result_type=StringValue,
        options_for=_options_for_fetch_prices,
    )
    async def fetch_prices(
        self, request: StringValue, ctx: object = None
    ) -> StringValue:
        """Fetch prices for a single symbol — one workflow per (symbol, run_id)."""
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id=f"vendor:{request.value}"),
            request,
            StringValue,
            _vendor_fetch,
        )

    @wrap_workflow_method(
        store=_store_of,
        result_type=StringValue,
        options_for=_options_for_compose_signal,
    )
    async def compose_signal(
        self, request: StringValue, ctx: object = None
    ) -> StringValue:
        """Compose a signal across many symbols — uses asyncio.gather to fan
        out parallel per-symbol workflows. Each per-symbol fetch lives in its
        own activity record, so partial failures only need to retry the
        failing symbol on the next invocation.
        """
        symbols = ["AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN", "META", "AMD"]
        annotate("symbols", str(len(symbols)))

        async def fetch_one(symbol: str) -> StringValue:
            return await current_workflow().execute_activity(
                ActivityOptions(activity_id=f"fetch:{symbol}"),
                StringValue(value=symbol),
                StringValue,
                _vendor_fetch,
            )

        prices = await asyncio.gather(*(fetch_one(s) for s in symbols))
        joined = StringValue(value=",".join(p.value for p in prices))
        return await current_workflow().execute_activity(
            ActivityOptions(activity_id="compose:signal"),
            joined,
            StringValue,
            _compose,
        )


async def main() -> None:
    operator = opendal.AsyncOperator(
        "fs", root=tempfile.mkdtemp(prefix="temporaless-quant-svc-")
    )
    store = OpenDALStore(operator)
    service = QuantService(store)

    print("=== single-symbol workflow (fetch_prices) ===")
    aapl = await service.fetch_prices(StringValue(value="AAPL"))
    print(f"  result: {aapl.value!r}")

    print("\n=== same call replays from storage ===")
    aapl_again = await service.fetch_prices(StringValue(value="AAPL"))
    print(f"  result: {aapl_again.value!r} (no vendor call)")

    print("\n=== fan-out workflow (compose_signal) ===")
    signal = await service.compose_signal(StringValue(value="batch-1"))
    print(f"  result: {signal.value!r}")

    print("\n=== signal replay short-circuits all 8 fetches + compose ===")
    signal_again = await service.compose_signal(StringValue(value="batch-1"))
    print(f"  result: {signal_again.value!r} (no vendor calls)")


if __name__ == "__main__":
    asyncio.run(main())
