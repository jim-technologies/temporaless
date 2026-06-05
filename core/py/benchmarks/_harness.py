"""Tiny benchmark harness that mirrors Go's ``testing.B`` style.

Each benchmark is an ``async def`` that takes a ``Bench`` and runs ``b.n``
iterations. The harness picks ``b.n`` to drive wall time toward ``target_seconds``
(default 1s), then prints one line per benchmark in Go's format::

    BenchmarkName                                   N    ns/op

Sync OpenDAL ops live inside these async functions because the framework's
storage layer is sync today; the async signature exists so benchmarks calling
``await run(...)`` and benchmarks calling sync ``store.put_*`` share the same
harness.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass
class Bench:
    n: int = 1
    _start_ns: int = 0

    def reset_timer(self) -> None:
        """Reset the elapsed-time clock so per-iteration setup doesn't count."""
        self._start_ns = time.perf_counter_ns()


BenchFunc = Callable[[Bench], Awaitable[None]]


async def _measure(fn: BenchFunc, n: int) -> int:
    bench = Bench(n=n)
    bench._start_ns = time.perf_counter_ns()
    await fn(bench)
    return time.perf_counter_ns() - bench._start_ns


async def run_benchmark(name: str, fn: BenchFunc, *, target_seconds: float = 1.0) -> None:
    target_ns = int(target_seconds * 1_000_000_000)
    n = 1
    last_n, last_ns_per_op = 1, 0
    for _ in range(30):
        elapsed = await _measure(fn, n)
        last_n = n
        last_ns_per_op = elapsed // max(n, 1)
        if elapsed >= target_ns:
            break
        if elapsed <= 0:
            n *= 100
            continue
        estimate = int(n * (target_ns / elapsed) * 1.2)
        n = max(n + 1, min(estimate, n * 100))
        if n > 100_000_000:
            break
    print(f"{name:<55s} {last_n:>10d}  {last_ns_per_op:>10d} ns/op")


def main(*entries: tuple[str, BenchFunc]) -> None:
    """Run each ``(name, fn)`` entry once via a single asyncio loop."""

    async def _runall() -> None:
        for name, fn in entries:
            await run_benchmark(name, fn)

    asyncio.run(_runall())
