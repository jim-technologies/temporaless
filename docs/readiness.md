# Architecture Review And Readiness

Temporaless implements most core primitives for bounded, serverless workflow
replay. **Unattended production operation still requires additional pieces**:
reliable invocation, wake delivery, side-effect idempotency, crash recovery,
and downstream projections remain application or deployment responsibilities.
Go and async Python are the first-class execution targets.

The architecture is deliberately small: an application-owned unary protobuf
handler runs against authoritative protobuf point records at deterministic
flat keys. Completed boundaries replay; pending work executes current code.
ConnectRPC exposes remote boundaries. Optional CloudEvents feed downstream
logs and indexes; Iceberg is the conventional analytical target. Neither an
index nor an analytical database participates in core replay.

| Critical piece | Readiness and evidence |
|---|---|
| Workflow and activity replay | Shipped, including terminal failure replay, stable caller-owned identities, and type-conflict checks. [Go runtime](../core/go/workflow/workflow.go), [Python runtime](../core/py/src/temporaless/workflow.py). |
| Activity retries | Shipped bounded retry policies, attempt records, and durable long backoffs with repair of interrupted timer/activity writes. [Retry repair tests](../core/go/workflow/retry_timer_repair_test.go), [Python retry tests](../core/py/tests/test_durable_retry.py). |
| Durable sleep and polling | Shipped protobuf timer records and typed pending results. Scanners discover due work; a deployment must invoke the owning application handler. [Scheduling contract](scheduling.md), [Go scanner](../adapters/go/timerscanner/scanner.go). |
| Event delivery | Shipped create-once delivery, duplicate acceptance, and conflict rejection when the store advertises atomic create. Direct Go OpenDAL does not provide this capability. [Go delivery](../core/go/storage/events.go), [hard cases](hard-cases.md#event-delivery-races). |
| Claims and concurrency | Shipped workflow/activity claims and concurrency slots on capable backends. Current claims are create-only; expired claims cannot be taken over automatically. [Claim contract](claims.md), [execution claim tests](../core/go/workflow/execution_claim_test.go). |
| Protobuf handler integration | Shipped options-driven wrappers, generated storage/query RPCs, and ConnectRPC adapters. [Go wrappers](../core/go/workflow/wrap.go), [application boundary](canonical-workflows.md). |
| Operator tools | Shipped inspection, explicit resets, backfill, timer scanning, and retention helpers. Their cadence and safe operating conditions remain deployment-owned. [Operator CLI](operator-cli.md), [runbook](runbook.md). |
| Logging and index notifications | Optional Go/Python storage-server interceptors emit CloudEvents containing protobuf keys after successful mutation RPCs. Publication is best-effort. [Go interceptor](../adapters/go/cloudevents/interceptor.go), [Python interceptor](../adapters/py/cloudevents/src/temporaless_cloudevents/adapter.py). |
| Query and analytical projections | Optional SQLite reference index is shipped. ClickHouse and Iceberg have a documented integration contract; production projectors, catalog wiring, and live backend conformance tests are not shipped. [SQLite adapter](../adapters/py/indexstore/README.md), [projection contract](clickhouse-iceberg.md). |

The remaining critical responsibilities are explicit design boundaries, not
features that a deployment can assume are already provided:

1. **Durable invocation and wakes.** Supply a durable queue or equivalent
   retryable handoff, cron trigger, due-timer dispatch, and application routing.
   Events and dependency completions need another invocation or opted-in
   durable polling. A successful enqueue and a completed workflow are distinct
   acknowledgements; remote `UNAVAILABLE` alone cannot distinguish a durable
   wait from infrastructure failure. The bundled dispatcher's default queue
   is process-local and best-effort. [Queue contract](scheduling.md#external-scheduler-and-queue-contract),
   [dispatch adapter](../adapters/go/dispatch/README.md).
2. **Safe external side effects.** Claims cannot close the crash window between
   an external write and its result checkpoint. Use domain idempotency or an
   application outbox. Keep activities small enough for the hosting platform;
   remote worker scheduling, heartbeats, and fenced long-running execution
   require an appropriate adapter. [Side effects](hard-cases.md#side-effects).
3. **Crash claim recovery.** Verify that the previous worker is gone before
   deleting a stranded create-only claim. Lease expiry and matching owner IDs
   do not prove it is safe to resume. CAS renewal and takeover are not
   implemented. [Lease lifecycle](claims.md#lease-lifecycle).
4. **Bounded scans and retention.** Core due-ledger and run-scoped listings
   materialize their selected namespace or run. Bound those scopes, preserve
   active timers through the recovery horizon, and compact ledger tombstones
   offline with writers/scanners quiesced. Reset and retention deletion also
   require quiescence; they are not execution fences. [Scan limits](hard-cases.md#point-store-scan-size),
   [retention](analytics.md#retention), [recovery procedures](runbook.md).
5. **Stable schedule progress.** Fresh one-shot schedulers need configured
   bootstrap anchors. `_latest` pointers are recovery hints: cross-process
   read/compare/write races can regress them and repeat fires. Stable run IDs
   make terminal replays safe; a strict monotonic cursor needs externally
   serialized progress or native conditional updates. [Bootstrap contract](scheduling.md#bootstrapping-one-shot-cron-ticks).
6. **Compatible application changes.** Existing runs are not pinned to old
   executable builds. Preserve input, control flow, and boundary-ID meaning
   across replay; use new run identities or a quiesced reset for incompatible
   changes. [Application changes](architecture.md#application-changes).
7. **Reliable downstream projections.** CloudEvents are invalidations, not
   ordered mutation history. Direct writes, internal repairs, partial failures,
   process exits, and publication failures can leave gaps. Consumers hydrate
   authoritative records and reconcile periodically. Required complete audit
   history needs a separately durable publication mechanism; there is no
   shipped durable outbox or complete historical event log. [CloudEvents adapter](../adapters/py/cloudevents/README.md),
   [projection ordering](clickhouse-iceberg.md#projection-ingestion).
8. **Richer workflow control.** Parent/child lifecycle supervision and durable
   operator cancellation are not shipped core primitives. Applications needing
   those semantics must supply them or use a compatible orchestrator; ordinary
   dependency polling and process cancellation do not provide them.
   [Capability comparison](comparisons.md), [compatibility scope](temporal-adapter.md).

This review fixes a Go activity-claim leak when the authoritative refresh fails
before the activity body starts, and corrects the deployment examples to seed
fresh one-shot cron processes from stable bootstrap anchors. These changes
remove avoidable stalls; they do not remove the recovery responsibilities
above. [Claim regression tests](../core/go/workflow/activity_claim_test.go),
[deployment recipe](deployment.md#cron-scheduler),
[runnable cron example](../examples/py/stocks_cron.py).

For one pass through the design, read [architecture](architecture.md),
[canonical application workflows](canonical-workflows.md),
[scheduling](scheduling.md), [hard cases](hard-cases.md), and
[the projection contract](clickhouse-iceberg.md), then apply the
[production checklist](production-checklist.md). The [SDK matrix](sdks.md)
records the narrower capabilities of the additional language packages.
