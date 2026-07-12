# Production Checklist

A pre-launch checklist for running Temporaless in production. Pair this with [`docs/deployment.md`](deployment.md) (architecture choices) and [`docs/hard-cases.md`](hard-cases.md) (known sharp edges).

The shape of the framework is *very thin*: there is no engine to operate, no control plane to keep alive. What you actually run is your own ConnectRPC service plus an object-storage bucket. This checklist is mostly about your service, with a few storage-specific items.

## Storage backend

- [ ] **Cloud storage with native atomicity.** S3, GCS, or Azure Blob. The OpenDAL `fs` scheme is **dev-only** — it cannot be safely shared across processes (see `docs/hard-cases.md`).
- [ ] **Bucket isolated per environment.** `temporaless-prod-{namespace}`. Disaster-recovery bucket replication if your RPO < your bucket-region RPO.
- [ ] **Versioning enabled.** Lets you recover from accidental record deletes (a janitor mis-configuration, a bad inspector script).
- [ ] **Lifecycle rules sized for your retention.** Bucket lifecycle is the bucket-only retention mechanism. If you need exact "COMPLETED older than max_age" deletion, deploy the optional query index and run `janitor.sweep`.
- [ ] **Encryption at rest.** SSE-KMS (AWS), CMEK (GCS), customer-managed keys (Azure) per your compliance posture.
- [ ] **Bucket-level audit logging.** S3 Access Logs / Cloud Audit Logs / Azure diagnostic logs. Records are protobuf binaries, so this lets you reconstruct who-touched-what without trusting the application.

## ConnectStore service

- [ ] **TLS terminated.** Either at the load balancer (typical) or via uvicorn's `ssl_certfile`/`ssl_keyfile`. Never expose ConnectRPC over plaintext on the public network.
- [ ] **Auth interceptor wired.** Bearer token / mTLS / OIDC / your service mesh of choice. The framework does not ship a default — it's a `MetadataInterceptor` Protocol you implement. See `examples/py/production_server.py` for a complete bearer-token implementation.
- [ ] **Rate limiting.** A `MetadataInterceptor` that rejects with `Code.RESOURCE_EXHAUSTED` past N requests per second per principal. The framework does not ship one because the right algorithm depends on your fairness model (token bucket / GCRA / fixed window).
- [ ] **Timeouts.** Set client-side via `ConnectStore.from_address(..., timeout_ms=…)`; set server-side via uvicorn's `timeout_keep_alive`/`timeout_graceful_shutdown`. Storage RPCs should comfortably complete in <1s on cloud backends; tune to your p99.
- [ ] **`/healthz` (liveness)** returns 200 unconditionally once the process is alive. Your supervisor (Lambda runtime, Cloud Run, systemd, K8s, ...) restarts the process if this fails. No auth — probes are unauthenticated.
- [ ] **`/readyz` (readiness)** returns 200 only after store + scheduler are initialized AND while the server is accepting traffic. Returns 503 during startup and during graceful shutdown so the load balancer drains traffic before the process exits.
- [ ] **Graceful shutdown on SIGTERM.** Set `/readyz` to fail first, wait the platform's grace window (typically 30s) for in-flight RPCs to finish, then exit.
- [ ] **Structured JSON logs.** One line per RPC outcome with code + elapsed + correlation_id. Forwarded to your aggregator (Loki / Datadog / CloudWatch). The framework does not configure a logger — wire one in your entrypoint.
- [ ] **Correlation ID per request.** The auth interceptor is a natural place to generate / read it (`x-correlation-id` header). Carry through all inner logs via a `ContextVar`.

## Workflow service (your trigger surface)

- [ ] **Same auth interceptor surface.** Your `WorkflowService` is just another ConnectRPC service — same interceptor list as ConnectStore.
- [ ] **Direct path preserved for normal APIs.** Application services keep ordinary API reads and routine synchronous actions callable in-process without Temporaless. Workflow wrappers are opt-in for idempotent, retriable, scheduled, or long-running operations; if Temporaless storage or operators are down, direct APIs still serve and only the durable operation returns an explicit unavailable/deferred result.
- [ ] **`workflow_id` and `run_id` are caller-provided.** The framework rejects empty / ambiguous IDs. Document your conventions: typically `{pipeline}:{symbol_or_partition}` for `workflow_id`; `{date}` or `{fire_time_iso}` for `run_id`.
- [ ] **`code_version` bumped on every breaking workflow body change.** Otherwise existing run records replay against new code → `WorkflowConflictError`. Convention: tie to git short-SHA or semver.
- [ ] **Activity bodies idempotent.** Stored terminal results replay, but retries, crashes, and unclaimed concurrent execution may run a body again. External side-effects (vendor calls, DB writes) must tolerate at-least-once delivery. The framework's claim system suppresses cooperating live duplicates but cannot extend exactly-once guarantees across vendor boundaries.

## Operator processes

- [ ] **Cron scheduler ticked from a reliable trigger.** EventBridge / Cloud Scheduler / GitHub Actions schedule / cron(8) / a K8s CronJob / an in-process `while True: tick(); sleep(60)` loop — pick whichever fits your platform. The scheduler tick is stateless; multiple copies provide at-least-once dispatch, so the resulting workflow calls must use execution claims or tolerate overlap.
- [ ] **Timer scanner ticked at the granularity you need.** ~1-minute polling is the framework's expected cadence. Tighter loops increase storage RPCs without lowering wake-up latency below the `sleep(duration)` precision you persisted.
- [ ] **Janitor running on a schedule** only if you deploy a query index for exact retention. Otherwise configure bucket lifecycle rules. Wire the deployment's `ClaimRunStore` so run-scoped claims are removed before records, and externally quiesce eligible runs during the nontransactional sweep. Daily / weekly per your retention policy.

## Observability

- [ ] **Metrics from interceptors, not from a parallel framework surface.** Wrap your interceptor list with the metrics middleware your service mesh provides (OpenTelemetry-Connect, Datadog interceptor, etc.). The framework deliberately does not ship `Observer` / `Tracer` Protocols — the gRPC interceptor is the universal seam.
- [ ] **Trace context propagated from interceptors into activity bodies** if you want activity-level spans. Inside the activity body, use your tracer SDK directly (`with tracer.start_as_current_span(...)`).
- [ ] **Bucket access metrics watched.** Storage cost = `(records/day) × (avg record size)` per record kind. Use storage-class transitions to manage long-tail cost.

## Failure modes and recovery

- [ ] **Workflows in `IN_PROGRESS` past their expected duration are alerted on.** A workflow stuck `IN_PROGRESS` for hours past its timer's `wake_at` indicates a stuck timer-scanner or a pending-event that nothing's delivering. Use the query index plus `inspector.list_in_flight_workflows(query_store)` to enumerate.
- [ ] **Failed workflow index reviewed regularly.** Failed runs are a queue of incidents. Use `RecordQueryService.ListWorkflows(status=FAILED)` and `inspector.reset_workflow(...)` to clear before re-running.
- [ ] **Disaster recovery runbook.** "Bucket gone" → restore from versioning / cross-region replica. "Process crash mid-write" → records are append-only per key, partial writes don't corrupt prior records (object-storage atomicity).
- [ ] **`ClaimBusy` budget watched.** A spike in `Code.ALREADY_EXISTS` indicates a live holder, a thundering herd, or a leaked create-only claim. Lease expiry does not free create-only claims; verify the owner is gone and delete the stale claim manually. CAS takeover is future-only.

## Container image (optional)

The bundled `Dockerfile` is multi-stage (Python 3.13-slim, ~140MB final image) — useful if your platform takes a container (Lambda container images, Cloud Run, Modal, Fly Machines, plain ECS). It's a starting point; for your own service:

- Replace the `CMD` line with your entrypoint.
- Use a digest-pinned base image (`python:3.13-slim@sha256:…`).
- Non-root user (`uid 10001` in the bundled Dockerfile).
- `HEALTHCHECK` wired to `/readyz`.
- Configure your platform's grace window so SIGTERM has ~30s to drain.

If your platform takes a Python module directly (Lambda zip, Modal, Fly), skip the Dockerfile and ship your code directly.

## Deployment platforms

The framework is platform-agnostic. The pieces you actually deploy:

- **Workflow service** (your gRPC handlers + `wrap_workflow_method` / `HandleConnect`) — any HTTP server runtime: Lambda + API Gateway, Cloud Run, Modal, Fly Machines, plain VMs, K8s if you must.
- **Cron scheduler tick** — EventBridge schedule, Cloud Scheduler, GitHub Actions schedule, K8s CronJob, plain cron(8). The scheduler is stateless.
- **Timer scanner tick** — same as above; ~1-minute cadence.
- **Janitor (optional)** — daily / weekly cron.
- **Storage** — S3, GCS, Azure Blob.

There is no engine to run, no control plane to operate. Pick whichever platform your team already operates.

## What you do not need

- **A workflow database.** Records ARE the state. No Postgres / Cassandra / Redis is required for execution; a query index is optional search infrastructure.
- **A scheduler binary.** The cron scheduler is a Python class you tick from any reliable cron source.
- **A control plane.** No service registry, no leader election, no quorum.
- **Sticky routing.** Terminal RPCs against the same `(workflow_id, run_id)` produce the stored result via replay. Round-robin live execution requires `claim_owner_id` plus an atomic claim store for single-flight; otherwise overlapping calls are at-least-once.
