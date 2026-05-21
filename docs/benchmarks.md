# Benchmarks

All three SDKs ship a benchmark suite covering the storage hot paths. Output
format matches Go's `testing.B` (`BenchmarkName N ns/op`) so cross-language
costs are directly comparable.

```sh
flox activate -- scripts/bench-go    # Go:     go test -bench=. on core/go/storage + core/go/workflow
flox activate -- scripts/bench-py    # Python: python -m benchmarks (auto-scales N toward ~1s wall time)
flox activate -- scripts/bench-rs    # Rust:   cargo build --release && ./target/release/bench-storage
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

These are illustrative — backend latency dominates everything once you point
at S3 / GCS. The `fs` backend exercises the encode/decode + filesystem path,
which is the SDK overhead a real deployment pays per record regardless of
remote-store latency.

Most recent run (Intel Xeon E5-2696 v4, `fs` backend, single-threaded):

| Benchmark | Rust (ns/op) | Go (ns/op) | Python (ns/op) | Notes |
|---|---:|---:|---:|---|
| `PutGetWorkflow` | **269,255** | 506,535 | 15,914,993 | Rust ~1.9× Go, ~59× Python. |
| `PutGetActivity` | **271,970** | 537,957 | 18,834,757 | Same ordering. |
| `ListActivitiesUnderRun_100` (Rust only) | 15,566,315 | — | — | 100 activities under one run — proportional. |
| `EncodeActivity` (Rust only) | **121** | — | — | Pure CPU: prost encode is microseconds. |
| `DecodeActivity` (Rust only) | **315** | — | — | Same — decode is slightly slower than encode. |

### What the numbers actually mean

- **Rust < Go < Python**, as expected. OpenDAL is Rust-native; the Go SDK
  calls into it via FFI (`opendal-go-services`, `purego`); the Python SDK
  goes through PyO3. Each binding layer adds overhead.
- **The Rust SDK is ~1.9× faster than Go** on the put-get round-trip. For
  the framework's typical throughputs (cron-driven workflows, webhook
  receivers), this is rarely the bottleneck. But for very-high-rate ingest
  paths it's a real win.
- **Python is ~31× slower than Go and ~59× slower than Rust**. Per-record
  overhead is dominated by protovalidate + Python's pure-Python protobuf
  encode. For LLM / vendor / quant workloads where you're waiting on
  network round-trips, this overhead disappears in the noise. For a pure
  storage-throughput service, prefer Go or Rust.
- **Pure protobuf encode/decode in Rust is sub-microsecond** (121 / 315 ns).
  Whenever your `ns/op` number is much larger than that, the cost is in the
  filesystem path, not the codec.

## What each SDK ships today

| | Go | Python | Rust |
|---|---|---|---|
| `OpenDALStore` | ✓ | ✓ | ✓ |
| `workflow.Run` / activity replay | ✓ | ✓ | not yet |
| Durable retry backoffs | ✓ | ✓ | not yet |
| Concurrency keys | ✓ | ✓ | not yet |
| Claims, cron scheduler, timer scanner, janitor | ✓ | ✓ | not yet |
| ConnectStore client / server | ✓ | ✓ | not yet |
| Adapters (backfill, dependencies, outbox, …) | ✓ | partial | — |

The Rust SDK is **storage-layer only** right now — it reads and writes the
same wire-format records the Go and Python SDKs use, so Rust-native
tooling (analytics CLIs, MCP servers, future Rust workflow runtime)
interoperates with workflows authored in either of the other languages.
The workflow runtime, adapters, and ConnectStore layers will be added as
the Rust SDK matures.

## Choosing a language

- **Throughput-bound triggers** (high-rate webhooks, low-latency RPC): Go
  or Rust. Rust is fastest at the storage layer but only ships read/write
  today; pick Go if you need the full runtime now.
- **I/O-bound workloads where you wait on vendor APIs** (LLM completions,
  vendor data fetches, market-data polling): Python is fine — the
  per-record overhead is dwarfed by network round-trips.
- **Polyglot deployments**: workflow records are protobuf-binary, so
  Go-written records are Python-readable and Rust-readable, and vice
  versa. A common pattern is Python workflow authoring + Rust analytics /
  inspector tooling.

## Adding a benchmark

All three suites are tiny.

- Go: drop `BenchmarkX(b *testing.B)` into `core/go/{storage,workflow}/benchmark_test.go`.
- Python: add an `async def bench_x(b: Bench)` to `core/py/benchmarks/bench_{storage,workflow}.py` and register it in `core/py/benchmarks/__main__.py`. The harness handles N-scaling, timing, and output formatting.
- Rust: drop an `async fn bench_x()` into `core/rs/temporaless/benches/storage.rs` and call it from `main()`. The 60-line harness inside that file auto-scales N to ~1s wall time and prints in Go's testing.B format.

The harness in `core/py/benchmarks/_harness.py` is ~60 lines — read it before adding anything elaborate.
