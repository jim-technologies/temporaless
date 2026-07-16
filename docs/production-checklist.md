# Production Checklist

A pre-launch checklist for running Temporaless in production. Pair this with [`docs/deployment.md`](deployment.md) (architecture choices) and [`docs/hard-cases.md`](hard-cases.md) (known sharp edges).

The shape of the framework is *very thin*: there is no engine to operate, no control plane to keep alive. What you actually run is your own ConnectRPC service plus an object-storage bucket. This checklist is mostly about your service, with a few storage-specific items.

## Storage backend

- [ ] **Cloud storage with native atomicity.** S3, GCS, or Azure Blob. The OpenDAL `fs` scheme is **dev-only** — it cannot be safely shared across processes (see `docs/hard-cases.md`).
- [ ] **Bucket isolated per environment.** `temporaless-prod-{namespace}`. Disaster-recovery bucket replication if your RPO < your bucket-region RPO.
- [ ] **Versioning enabled.** Lets you recover from accidental record deletes (a janitor mis-configuration, a bad inspector script).
- [ ] **Lifecycle rules preserve live wakes.** Never expire active run records
  or `temporaless/v2/{namespace}/_due/` entries before the maximum timer
  horizon plus the longest tolerated scheduler outage/recovery window. If
  timers are unbounded, exempt both from lifecycle deletion. Use the optional
  query index plus `janitor.sweep` for exact "COMPLETED older than max_age"
  deletion.
- [ ] **Encryption at rest.** SSE-KMS (AWS), CMEK (GCS), customer-managed keys (Azure) per your compliance posture.
- [ ] **Bucket-level audit logging.** S3 Access Logs / Cloud Audit Logs / Azure diagnostic logs. Records are protobuf binaries, so this lets you reconstruct who-touched-what without trusting the application.

## ConnectStore service

- [ ] **TLS terminated.** Either at the load balancer (typical) or via uvicorn's `ssl_certfile`/`ssl_keyfile`. Never expose ConnectRPC over plaintext on the public network.
- [ ] **Auth enforced before body consumption.** Bearer token / mTLS / OIDC /
  your service mesh of choice. In Python, a Connect `MetadataInterceptor` runs
  before the service method but after the runtime has read the unary body, so
  use outer ASGI/ingress authentication as the resource-exhaustion boundary.
  See `examples/py/production_server.py` for a complete implementation.
- [ ] **Authorization is per RPC and least privilege.** The production-server
  bearer-token examples intentionally model one trusted internal principal;
  they are not tenant isolation or an operator authorization policy. Give
  workflow runtimes and human/automation operators separate identities, and
  reserve reset, delete, sweep, claim cleanup, and timer-repair mutations for
  the operator identity.
- [ ] **Server configuration fails closed.** The Python example requires
  `AUTH_TOKEN` and `TEMPORALESS_STORAGE_SCHEME`; backend options are a JSON
  string map in `TEMPORALESS_STORAGE_OPTIONS` (empty by default). It rejects
  ephemeral `memory` storage and requires
  `TEMPORALESS_ALLOW_UNSAFE_FS=1` for local `fs`. The Go
  example requires `AUTH_TOKEN`, `TEMPORALESS_STORAGE_ROOT`, and
  `TEMPORALESS_ALLOW_UNSAFE_FS=1`;
  replace that example's filesystem-only store for distributed production.
- [ ] **Request limits at every layer.** The production examples cap encoded
  HTTP request bodies and decoded Connect messages at 8 MiB. Keep finite body,
  header-read, body-read, write, keep-alive, and graceful-shutdown timeouts in
  the application server and ingress. Tune the byte ceiling deliberately if
  your protobuf contract genuinely requires larger records.
- [ ] **Compressed-request bombs blocked at ingress.** The application raw-body
  cap limits encoded bytes, but connectrpc-python currently joins and
  decompresses a request before applying its decoded-message limit. Configure
  the reverse proxy to cap encoded bodies and read duration, and either reject
  request compression or enforce a decompressed-size/expansion-ratio ceiling.
  Do not rely on the ASGI application alone for slow uploads or decompression
  bombs.
- [ ] **Rate limiting.** Apply a coarse unauthenticated limit at the ingress
  and a principal-aware limit after authentication. The framework does not
  choose an algorithm because the right fairness model (token bucket / GCRA /
  fixed window) is deployment-specific.
- [ ] **Timeouts.** Set client-side via `ConnectStore.from_address(..., timeout_ms=…)`; keep application-server and ingress read/write/idle/graceful-shutdown timeouts finite. Storage RPCs should comfortably complete in <1s on cloud backends; tune to your p99.
- [ ] **`/healthz` (liveness)** returns 200 unconditionally once the process is alive. Your supervisor (Lambda runtime, Cloud Run, systemd, K8s, ...) restarts the process if this fails. No auth — probes are unauthenticated.
- [ ] **`/readyz` (readiness)** returns 200 only after the ConnectStore server
  and storage adapter are initialized and while the server is accepting
  traffic. It returns 503 during startup and graceful shutdown. Operators have
  their own readiness signal; the storage-only production server does not run
  a scheduler.
- [ ] **Graceful shutdown on SIGTERM.** Set `/readyz` to fail first, wait the platform's grace window (typically 30s) for in-flight RPCs to finish, then exit.
- [ ] **Structured JSON logs.** One line per RPC outcome with code + elapsed + correlation_id. Forwarded to your aggregator (Loki / Datadog / CloudWatch). The framework does not configure a logger — wire one in your entrypoint.
- [ ] **Correlation ID per request.** The outer auth middleware is a natural
  place to generate/read it (`x-correlation-id` header). Carry it through inner
  logs via a `ContextVar`.

## Workflow service (your trigger surface)

- [ ] **Same outer-auth and interceptor surfaces.** Your `WorkflowService` is
  just another ConnectRPC service: protect it with the same ingress/HTTP
  boundary and RPC-aware interceptors as ConnectStore.
- [ ] **Direct path preserved for normal APIs.** Application services keep ordinary API reads and routine synchronous actions callable in-process without Temporaless. Workflow wrappers are opt-in for idempotent, retriable, scheduled, or long-running operations; if Temporaless storage or operators are down, direct APIs still serve and only the durable operation returns an explicit unavailable/deferred result.
- [ ] **`workflow_id` and `run_id` are caller-provided.** The framework rejects empty / ambiguous IDs. Document your conventions: typically `{pipeline}:{symbol_or_partition}` for `workflow_id`; `{date}` or `{fire_time_iso}` for `run_id`.
- [ ] **`code_version` bumped on every breaking workflow body change.** Otherwise existing run records replay against new code → `WorkflowConflictError`. Convention: tie to git short-SHA or semver.
- [ ] **Activity bodies idempotent.** Stored terminal results replay, but retries, crashes, and unclaimed concurrent execution may run a body again. External side-effects (vendor calls, DB writes) must tolerate at-least-once delivery. The framework's claim system suppresses cooperating live duplicates but cannot extend exactly-once guarantees across vendor boundaries.

## Operator processes

- [ ] **Cron scheduler ticked from a reliable trigger.** EventBridge / Cloud
  Scheduler / GitHub Actions schedule / cron(8) / a K8s CronJob / an in-process
  loop — pick whichever fits your platform. Restore its in-memory last-fire
  cache from latest-run pointers or an external snapshot before ticking, and
  set every dispatched `WorkflowOptions.run_order_time` to its fire time.
  Multiple copies provide at-least-once dispatch, so resulting workflow calls
  must use execution claims or tolerate overlap.
- [ ] **Timer scanner ticked at the granularity you need.** ~1-minute polling is the framework's expected cadence. Tighter loops increase storage RPCs without lowering wake-up latency below the `sleep(duration)` precision you persisted.
- [ ] **Point-store scan partitions are bounded.** Core `DueTimers` materializes
  its selected namespace, and run-scoped activity/timer/event/claim lists
  materialize the selected run; these RPCs are intentionally unpaginated.
  Partition namespaces and avoid enormous aggregate runs. Use an indexed due
  query or external scheduler for very large timer backlogs.
- [ ] **Timer delivery treated as at-least-once.** A due wake remains
  dispatchable until replay persists a later wake-bearing or terminal boundary;
  multiple scanners can dispatch it concurrently. Use execution claims to
  single-flight cooperating workflow invocations and keep external side
  effects idempotent.
- [ ] **Operators run beside the application trigger, not inside ConnectStore.**
  The production-server examples serve records only. Wire `BackgroundWorkers`
  into the application workflow service, deploy a separate operator with a
  real workflow dispatcher, or use an external scheduler/queue.
- [ ] **Every failed timer-scanner tick alerts.** Canonical repair failures and
  corrupt `_due` entries fail the tick rather than returning a partial success;
  inspect the deterministic `_due_invalid` forensic copy and restore or
  republish the exact timer before acknowledging the alert.
- [ ] **Janitor running on a schedule** only if you deploy a query index for
  exact retention. Otherwise use conservative bucket lifecycle rules that
  satisfy the live-wake retention constraint above. Wire the deployment's
  `ClaimRunStore` so run-scoped claims are removed before records, and
  externally quiesce eligible runs during the nontransactional sweep. Daily /
  weekly per your retention policy.
- [ ] **Due-ledger cleanup is offline and quiescent.** Generic `_due` entries
  are deterministic prepared records; terminal/canceled tombstones remain to
  suppress interrupted transitions. Stop timer writers/scanners and verify
  workflow, canonical timer, and prepared state before removing them. The
  generic ledger has no online compaction mode.

## Observability

- [ ] **Metrics from interceptors, not from a parallel framework surface.** Wrap your interceptor list with the metrics middleware your service mesh provides (OpenTelemetry-Connect, Datadog interceptor, etc.). The framework deliberately does not ship `Observer` / `Tracer` Protocols — the gRPC interceptor is the universal seam.
- [ ] **Trace context propagated from interceptors into activity bodies** if you want activity-level spans. Inside the activity body, use your tracer SDK directly (`with tracer.start_as_current_span(...)`).
- [ ] **Bucket access metrics watched.** Storage cost = `(records/day) × (avg record size)` per record kind. Use storage-class transitions to manage long-tail cost.

## Failure modes and recovery

- [ ] **Workflows in `IN_PROGRESS` past their expected duration are alerted on.** A workflow stuck `IN_PROGRESS` for hours past its timer's `wake_at` indicates a stuck timer-scanner or a pending-event that nothing's delivering. Use the query index plus `inspector.list_in_flight_workflows(query_store)` to enumerate.
- [ ] **Failed workflow index reviewed regularly.** Failed runs are a queue of
  incidents. Before a partial retry, quiesce the run, reset only failed child
  activity/paired retry-timer records, and delete the parent workflow record
  last. Then invoke once and resume dispatch. See `docs/runbook.md`.
- [ ] **Disaster recovery runbook.** "Bucket gone" → restore from versioning / cross-region replica. "Process crash mid-timer-write" → the deterministic prepared record overlays and repairs the canonical timer before a later scan dispatches it (object-storage point writes are atomic).
- [ ] **`ClaimBusy` budget watched.** A spike in `Code.ALREADY_EXISTS` indicates a live holder, a thundering herd, or a leaked create-only claim. Lease expiry does not free create-only claims; verify the owner is gone and delete the stale claim manually. CAS takeover is future-only.

## Container image (optional)

The bundled `Dockerfile` is multi-stage (digest-pinned Python 3.14.6-slim) — useful if your platform takes a container (Lambda container images, Cloud Run, Modal, Fly Machines, plain ECS). Its default command starts the ConnectStore-only Python example and therefore requires the explicit auth/storage environment described above. The image keeps `/app` root-owned while running as the unprivileged `app` user, so application code and the virtual environment cannot be modified by a compromised process. It's a starting point; for your own service:

- Replace the `CMD` line with your entrypoint.
- Keep the exact Python and uv image digests pinned; update tag and digest together.
- Keep application files root-owned and run as the non-root user (`uid 10001`
  in the bundled Dockerfile). Mount only the paths that genuinely need writes.
- Run with a read-only root filesystem. Provide a small `tmpfs` for `/tmp` if
  your platform or dependencies need it; provide a separate writable
  volume/tmpfs only for explicitly configured local state. Cloud object
  storage deployments should not need a writable application filesystem.
- Drop all Linux capabilities, set `no-new-privileges`, and keep the runtime's
  default seccomp/AppArmor (or an equivalently restrictive) profile enabled.
- Inject `AUTH_TOKEN` through the platform's secret manager at runtime; never
  bake it into an image layer or deployment manifest. The example's one shared
  bearer token demonstrates authentication for a single internal principal,
  not tenant-aware or per-RPC authorization. Put that policy at the ingress or
  replace the example guard for multi-principal deployments.
- `HEALTHCHECK` wired to `/readyz`.
- Configure your platform's grace window so SIGTERM has ~30s to drain.

If your platform takes a Python module directly (Lambda zip, Modal, Fly), skip the Dockerfile and ship your code directly.

## Deployment platforms

The framework is platform-agnostic. The pieces you actually deploy:

- **Workflow service** (your gRPC handlers + the Python/Go `connectworkflow` adapter) — any HTTP server runtime: Lambda + API Gateway, Cloud Run, Modal, Fly Machines, plain VMs, K8s if you must.
- **Cron scheduler tick** — EventBridge schedule, Cloud Scheduler, GitHub
  Actions schedule, K8s CronJob, plain cron(8). Seed the in-memory last-fire
  cache before each ephemeral process ticks.
- **Timer scanner tick** — same as above; ~1-minute cadence.
- **Janitor (optional)** — daily / weekly cron.
- **Storage** — S3, GCS, Azure Blob.

There is no engine to run, no control plane to operate. Pick whichever platform your team already operates.

## What you do not need

- **A workflow database.** Records ARE the state. No Postgres / Cassandra / Redis is required for execution; a query index is optional search infrastructure.
- **A scheduler binary.** The cron scheduler is a Python class you tick from any reliable cron source.
- **A control plane.** No service registry, no leader election, no quorum.
- **Sticky routing.** Terminal RPCs against the same `(workflow_id, run_id)` produce the stored result via replay. Round-robin live execution requires `claim_owner_id` plus an atomic claim store for single-flight; otherwise overlapping calls are at-least-once.
