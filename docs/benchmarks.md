# Benchmarks

Both languages ship a benchmark suite covering the storage and workflow hot paths. Output format matches Go's `testing.B` (`BenchmarkName N ns/op`) so cross-language costs are directly comparable.

```sh
flox activate -- scripts/bench-go    # Go: go test -bench=. on core/go/storage and core/go/workflow
flox activate -- scripts/bench-py    # Python: python -m benchmarks (auto-scales N toward ~1s wall time)
```

## What the suite covers

| Benchmark | What it measures |
|---|---|
| `BenchmarkPutGetWorkflow` | Round-trip put + get for a single `WorkflowRecord`. |
| `BenchmarkPutGetActivity` | Round-trip put + get for a single `ActivityRecord`. |
| `BenchmarkListWorkflowsScan/workflows={10,100,500}` | Walk the workflow tree at three scales. |
| `BenchmarkListWorkflowsScopedByID/{unscoped,scoped_by_workflow_id}` | List all 500 runs vs. one schedule's 10 runs. |
| `BenchmarkWorkflowRunFreshExecution` | Fresh `workflow.Run` with one activity from a clean store. |
| `BenchmarkWorkflowRunReplay` | Replay a completed workflow (fingerprint match → return stored result). |
| `BenchmarkRetryLoopInProcess` | Three-attempt retry loop with 1ms backoff. |

All benchmarks run against OpenDAL `fs` with a per-benchmark temp directory.

## Reference numbers

These are illustrative — backend latency dominates everything once you point at S3 / GCS. Measured on AMD Ryzen AI 9 365 (20-thread) with `fs` backend.

| Benchmark | Go (ns/op) | Python (ns/op) | Notes |
|---|---:|---:|---|
| `PutGetWorkflow` | 91,375 | 5,149,988 | Python pays for protovalidate + Python protobuf encode. |
| `PutGetActivity` | 119,786 | 5,988,403 | Same story. |
| `ListWorkflowsScan/workflows=10` | 827,755 | 19,472,650 | `fs` walk + per-record decode. |
| `ListWorkflowsScan/workflows=100` | 6,267,296 | 168,010,234 | Linear in record count, as expected. |
| `ListWorkflowsScan/workflows=500` | 37,740,744 | 867,044,803 | |
| `ListWorkflowsScopedByID/unscoped` (500 runs) | 45,508,087 | 925,394,122 | |
| `ListWorkflowsScopedByID/scoped_by_workflow_id` (10 runs) | 737,731 | 28,161,987 | **62× speedup in Go, 33× in Python.** Always pass `workflow_id` if you can. |
| `WorkflowRunFreshExecution` | 277,632 | 22,571,391 | One workflow record + one activity record + replay logic. |
| `WorkflowRunReplay` | 55,028 | 2,933,588 | Single fingerprint check + return. |
| `RetryLoopInProcess` (3 attempts, 1ms backoff) | 2,774,505 | 38,337,011 | Includes ~2ms of `time.Sleep` per attempt that's unavoidable. |

## Choosing a language

- **Throughput-bound triggers** (high-rate webhooks, low-latency RPC): Go.
- **I/O-bound workloads where you wait on vendor APIs** (LLM completions, vendor data fetches, Twitter polling): Python is fine — the per-record overhead is dwarfed by network round-trips.
- **Mixed deployments**: workflow records are protobuf-binary, so Go-written records are Python-readable and vice-versa. Pick per-workflow.

## Adding a benchmark

Both suites are tiny.

- Go: drop `BenchmarkX(b *testing.B)` into `core/go/{storage,workflow}/benchmark_test.go`.
- Python: add an `async def bench_x(b: Bench)` to `core/py/benchmarks/bench_{storage,workflow}.py` and register it in `core/py/benchmarks/__main__.py`. The harness handles N-scaling, timing, and output formatting.

The harness in `core/py/benchmarks/_harness.py` is ~60 lines — read it before adding anything elaborate.
