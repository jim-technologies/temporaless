# How Temporaless Compares

Honest positioning against the workflow frameworks teams actually evaluate. Read this before adopting — there are real cases where one of the others is the right call.

## TL;DR

| | Temporaless | Temporal | Prefect | Dagster | n8n |
|---|---|---|---|---|---|
| Trigger surface | **standard gRPC** | Temporal SDK | Python decorator | Python asset graph | visual editor |
| State backend | **your S3 / GCS** | Temporal server (postgres / cassandra) | Prefect server (postgres) | Dagster server (postgres) | n8n server (postgres) |
| Engine to deploy | **none** | yes (Temporal frontend + matching + history + worker) | yes | yes | yes |
| Languages | **Go + Python** | Go, Python, Java, .NET, TS | Python | Python | JS / no-code |
| Async-only Python | **yes** | optional | partial | no | n/a |
| Records auditable from outside | **yes (binary protobuf in object storage)** | no (opaque history events) | partial | partial | no |
| Workflow IDs | **caller-provided** | caller-provided | system-generated | system-generated | n/a |
| Long-running durable workflows (sleep, wait for event, resume across process death) | **yes** | yes | partial | partial | partial |
| Sub-second timer accuracy | no (polling, ~minute granularity) | **yes** (push from server) | no | no | no |
| Multi-signal channels / `select` semantics | no (one create-once payload per event_id) | **yes** | no | no | partial |
| Query RPCs against running workflows | no (read records instead) | **yes** | no | no | no |
| Workflow update RPCs (in-flight state mutation) | no | **yes** | no | no | no |
| Child workflows | no | **yes** | partial | no | partial |
| History rewinding to arbitrary point | no (delete records to reset boundaries) | **yes** | no | no | no |
| Visual builder | bring your own (typed plan + run projection supplied) | no | partial | no | **yes** |
| Lineage / asset graph | no | no | no | **yes** | no |
| Number of concepts to learn | **~6** (record kinds, store, retry, claim, schedule, annotation) | ~30+ | ~15 | ~20 | ~10 |

## Use Temporaless if

- Your trigger surface is already gRPC / ConnectRPC, or you'd benefit from making it gRPC. Wrapping a service method as a workflow is a one-line decorator.
- You want **zero engine to operate**. Records are protobuf files in S3/GCS; pods are interchangeable; horizontal scale is just "run more pods against the same bucket."
- Workloads are I/O-bound: LLM completions, vendor APIs, ML inference, quant data feeds. Async-first Python + structured parallel fan-out via `gather_activities` are first-class.
- You value **inspectability**: every workflow boundary is a `.binpb` file at a deterministic path. You can `aws s3 cp` it and `protoc --decode` it without running our code.
- You're shipping with a small ops team and don't want a control plane to operate.
- You need **long-running durable workflows** with simple wait patterns: durable sleep, single-event approvals, cron-driven retries. Process death between waits is a non-event — state lives in storage.

## Don't use Temporaless if

- You need **rich in-flight workflow interaction**: multi-signal channels with `select` semantics, query RPCs against running workflows, workflow update RPCs, child workflows, history rewinding to arbitrary points. Use **Temporal**. The `temporalcompat` adapters run Temporaless-shaped handlers on the real Temporal SDK; they do not emulate these features on Temporaless storage.
- You need **sub-second timer accuracy**. Our timer-scanner cadence is ~1 minute (set by your CronJob). Temporal pushes timer events from the server; ours polls.
- You think in **assets and lineage**, not workflows. A "compute artifact X from Y" mental model with first-class type system and asset metadata is **Dagster**'s job.
- You want a ready-made **visual builder and integrations catalog** for
  non-engineers. Use **n8n** or **Zapier**. Temporaless supplies a typed plan
  and plan-versus-actual projection for products building their own editor; it
  does not ship the editor.
- You already run **Prefect** in production at scale and want a managed UI / scheduler / cloud — switching costs aren't worth it.

## Where each tool wins

### Temporal

We **both** support long-running durable workflows. A workflow that sleeps 7 days and resumes works in either system. Where Temporal genuinely wins is **rich in-flight interaction patterns**:

- **Multi-signal channels with `select` semantics.** A Temporal workflow can wait on `cancel`, `extend`, `escalate`, `update_priority` simultaneously and react to whichever fires first. A Temporaless event key accepts one create-once payload and is reread during replay; it is not a channel. For multi-message bidirectional patterns (chat-shaped workflows, multi-step approvals with branching), Temporal is the right tool.
- **Query RPCs.** External code can ask a running Temporal workflow for its current state without waking it. We don't have queries — to inspect state, you read the records (still cheap, but async, not request/response).
- **Workflow update RPCs.** In-flight workflows can accept atomic state mutations from outside. Same gap as queries.
- **Child workflows.** First-class. We don't model parent/child workflow relationships.
- **Sub-second timer accuracy.** Their server pushes timer events; ours polls (typically every minute). Fine for quant/ML cadences, wrong for HFT or sub-second SLAs.
- **History rewinding to arbitrary points.** Reset to event N. We can delete record-by-record to reset specific boundaries, but not point-in-time replay of an in-flight workflow.

The cost of all that is operating the Temporal cluster (frontend + matching + history + worker, with Postgres or Cassandra). We deliberately don't try to compete on multi-actor orchestration patterns—but we **do** support long-running workflows. A 7-day approval workflow with one yes/no event fits Temporaless, with durable records and either application-triggered re-invocation or an explicit polling timer rather than Temporal's pushed history semantics. A 30-day support escalation with five signal channels and a query API for the dashboard is Temporal's job.

### Prefect

Wins on **the data-engineering ergonomics**: their UI, their flow run dashboard, their integrations library, their managed cloud. If your team already speaks Prefect and ships data pipelines on Prefect Cloud, the answer is keep using Prefect.

We compete only on the "I want decorator-shaped Python workflows but no separate server" axis. Storage-first vs Prefect-server is a real tradeoff: cheaper to operate, less batteries included.

### Dagster

Wins for **data-asset-first** thinking: software-defined assets, lineage graph, type-driven data dependencies, materialization tracking. If your model is "asset X is computed from assets Y and Z, and we need to know when each is stale," that's Dagster's whole identity.

We're workflow-graph, not asset-graph. We don't model lineage. If you need lineage, the framework is wrong for you.

### n8n

Wins for **no-code / low-code** glue work that non-engineers compose. Visual editor, hundreds of integrations, runs alongside Slack/Zapier in the same conceptual category.

We're code-first and do not ship n8n's editor or integration catalog. A
product can still put an n8n-style UI above Temporaless: boxes compile to
typed protobuf activities, sleeps, events, branches, and dependencies; an
optional `WorkflowPlan` is rendered for approval; the visualization adapters
join it to durable records. See
[`visual-workflows.md`](visual-workflows.md).

## What we deliberately don't ship

These would all be reasonable additions; we've chosen not to:

- **A UI / dashboard.** The S3 / GCS console is the dashboard. Records are the source of truth.
- **A scheduler service.** The cron scheduler is a Python class you call from a Kubernetes CronJob or a `while true: tick(); sleep(60s)` loop. There's no scheduler binary.
- **A control plane.** No "register a workflow definition" step. The decorator IS the registration.
- **An asset / lineage system.** Use Dagster.
- **Workflow query / update / signal-channel RPCs.** These are Temporal's conceptual model. Use a native Temporal workflow if you need them; `temporalcompat` deliberately does not emulate them.
- **A graphical workflow editor.** We ship the protobuf plan and record
  projection needed to build one, not the editor itself. Use n8n or Zapier
  when you want a finished no-code product.

## Data-pipelining patterns (Airflow / Luigi / Dagster / Prefect)

We're not aiming to compete head-on with these for asset-graph or lineage workloads — but the bread-and-butter "extract → transform → load with retries, branching, fan-out, backfills" cases all map to our primitives without ceremony. Here's how the common patterns translate. See `examples/py/data_pipeline.py` for an end-to-end runnable demo.

| Airflow / Luigi / Dagster / Prefect | Temporaless |
|---|---|
| Define a DAG of tasks with a declaration language (`@dag`, `@op`, Luigi `Task`, Prefect `@flow`) | A workflow body is the DAG. Sequential `await` for dependencies, `gather_activities` for fan-out, `if`/`else` for branches. Python control flow IS the DAG — no second declaration language to learn. |
| Each task is a function with retry config | Each step is an activity (`workflow.execute_activity(options, input, factory, body)`); retry policy lives on `ActivityOptions.retry_policy`. |
| Parallel tasks via `expand` (Airflow), `DynamicTasks` (Prefect), or asset graph (Dagster) | `gather_activities(*[execute_activity(...) for x in items])`. It settles every started branch before returning; each explicit activity_id gets its own record and replays independently. Go uses `AllActivities(ctx, calls...)` with the same semantics. |
| Bounded concurrency / pools (Airflow Pools) | `asyncio.Semaphore(n)` around each branch passed to `gather_activities`. Standard Python; not a framework primitive. |
| Conditional branching (Airflow `BranchPythonOperator`) | A regular `if`/`elif`/`else` around `execute_activity(...)` calls in the workflow body. |
| Idempotent re-runs / clear-and-rerun | Replay short-circuits per activity_id. For a failed partial retry, quiesce the run, delete the failed activity's validated paired retry timer and activity record, then delete the parent workflow record last. Successful activity records remain checkpoints. |
| Backfill across a date range | Python: `await temporaless.backfill.backfill(invoke, run_ids, concurrency=N, halt_on_error=...)`; Go: `backfill.Backfill[Resp](ctx, runIDs, backfill.Options{Concurrency: N, HaltOnError: ...}, invoke)`. Bounded concurrency, aggregated report with `succeeded()` / `failed()` / `pending()`, idempotent re-runs (COMPLETED replays from storage in microseconds). |
| Sensors (file-arrived, time-elapsed, upstream-finished) | `workflow.sleep(timer_id, duration)` for time; `workflow.wait_event(event_id, payload_type, poll_options)` for external triggers. Sleep always creates a durable wake. Event/dependency waits are manual by default; pass caller-owned `PollOptions` only when the timer scanner should recheck them. |
| Cross-DAG dependencies ("DAG B waits for DAG A's run") | Python: `await temporaless.dependencies.wait_for_workflow(store, workflow_id="A", run_id=date, result_factory=..., poll_options=...)`; Go: `dependencies.WaitForWorkflow(ctx, store, key, newResult, pollOptions)`. Returns A's typed result on COMPLETED; returns a typed pending result if A is unfinished and optionally arms a durable poll; returns `WorkflowDependencyFailedError` if A failed terminally. |
| Per-run parameters (Airflow `dag_run.conf`) | The protobuf request message IS the parameter bag. Strongly typed; caller-supplied `(workflow_id, run_id)` is the de-duplication key. |
| Operator UI / re-run from UI | No UI. With the optional query index, list failed runs and use the child-first reset procedure in `docs/runbook.md` from a script, notebook, or the shipped Invariant Protocol projection with operator methods explicitly enabled; then invoke the canonical application workflow RPC. |
| Scheduler service (Airflow scheduler, Prefect agent) | `cronscheduler` is a Python class you tick from a Kubernetes CronJob, EventBridge schedule, or `while True: tick(); sleep(60)` loop. No scheduler binary. |
| Asset graph / lineage (Dagster) | Not provided. Use Dagster if this is your model. |

**The mental shift coming from Airflow/Prefect/Dagster:**

- A DAG isn't declared — it's *expressed* as a workflow body using async/await. This means refactors are normal Python refactors; no separate "register the new task" step.
- "Tasks" are activities. Each gets a stable `activity_id` you choose —
  deterministic across re-runs. Reusing an `activity_id` replays its stored
  result even when new input bytes differ; choose a new ID when you intend a
  distinct execution. `ActivityConflictError` is reserved for incompatible
  request/response types or persisted retry/timer contracts.
- "DAG run" = `workflow_id + run_id`. Caller-provided. By convention, `run_id` embeds the fire time / partition / batch ID for backfill-friendliness.
- The scheduler keeps only a last-fire cache that can be restored from
  latest-run pointers or an external snapshot. It calls `run()` for each due
  schedule. Terminal duplicate dispatches replay. To serialize two schedulers
  racing on a first invocation, set `claim_owner_id` and use an atomic claim
  store; otherwise dispatch is at-least-once.

If your pipelining needs are **lineage-aware** (Dagster's whole identity), use
Dagster. If you need a finished **visual/no-code product**, use n8n or Zapier.
If you are building your own visual product, Temporaless provides a typed plan
and durable execution projection without requiring its UI to become the
runtime. Code-first ETL with retries, fan-out, backfill, and cross-pipeline
waits remains the native path.

## Migration paths

- **From Temporal**: unary protobuf activity/business bodies that do not call Temporal APIs can stay unchanged. Workflow orchestration still needs a mechanical rewrite for Temporaless activity dispatch, sleeps, events, and caller-owned IDs. `adapters/{go,py}/temporalcompat` is only the opposite, outbound direction: it runs Temporaless-shaped handlers on the real Temporal SDK.
- **From Prefect**: their `@flow` / `@task` model is conceptually similar to the `adapters/py/connectworkflow` decorator plus core `execute_activity`. Rewriting decorators is mechanical; storage cutover is the harder part.
- **From Airflow**: each task becomes an activity; the DAG body becomes a workflow body composed with `await`. `dag_run.conf` parameters become the protobuf request message. Pools become `asyncio.Semaphore`. The scheduler service becomes a `cronscheduler.Scheduler` you tick from a Kubernetes CronJob.
- **From Luigi**: each `Task.run()` becomes an activity; output `Target` becomes a stored protobuf record. Luigi's `requires()` chain becomes regular `await` in the workflow body.
- **From Dagster**: if your assets are simple "fetch → transform → persist" without complex lineage, the asset DAG can be rewritten as a workflow with N parallel activities. If Dagster remains the control plane, invoke the canonical Temporaless application RPC across a process boundary; current Dagster and Temporaless protobuf runtime requirements cannot share one supported Python environment. If your asset graph is rich, you probably should not migrate.
- **From n8n**: your nodes become activities and the trigger becomes the
  canonical protobuf RPC. This is still a compiler/rewrite, not import
  substitution, but the optional `WorkflowPlan` can preserve the visual
  topology and node IDs for approval and run projection.

## What "modern common cases" means here

The cases we explicitly target:
- Quant / market-data pipelines with cron-driven fetches and dependent computation.
- LLM completion workflows with retry on rate-limit, structured annotations.
- ML inference pipelines with parallel model fan-out.
- Webhook receivers that wait for external events (Twitter, Slack, custom).
- Idempotent ingest workers triggered by queues.
- Periodic batch jobs that need durable retries.
- Code-first ETL: extract → transform → validate → load with branching and backfill (the Airflow / Luigi sweet spot, minus the asset graph).

If you're outside this list, ask: do you really need a workflow framework, or do you need a queue + a database + good logging? Often the answer is the latter, and we'd rather you skip us than misuse us.
