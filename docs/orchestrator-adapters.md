# Orchestrator Adapters

Temporaless adapters preserve the unary protobuf handler convention. They do
not emulate another orchestrator's control plane, and they do not turn
arbitrary framework-native workflow code into Temporaless code by changing an
import.

## Direction

The shipped Temporal and Prefect adapters are outbound:

```text
Temporaless-shaped unary protobuf handler
        ├── real Temporal SDK worker
        └── real Prefect flow/task runtime
```

This is useful for a staged migration or for keeping an existing orchestrator's
worker/UI surface. The inverse direction would need to emulate framework
semantics such as Temporal history, signals and children, or Prefect task
runners and deployment state. Temporaless intentionally does not claim that.

Code reuse is still high when the business boundary is already:

```python
async def execute(request: Request) -> Response:
    ...
```

That body can be wrapped without changes when it has no framework-specific
context calls. Workflow orchestration needs a small explicit rewrite for
activity dispatch, durable waits, and caller-owned IDs.

## Prefect

`adapters/py/prefectcompat` wraps async unary protobuf handlers with the real
Prefect 3 flow/task APIs. It supports explicit name/retry options and encodes
workflow requests as deterministic protobuf binary inside a typed,
JSON-safe deployment parameter. A scheduled worker therefore reconstructs the
original protobuf request instead of Prefect replacing it with a display
placeholder.

Prefect owns its flow-run state, scheduling, UI, work pools, and automations.
Temporaless replay applies only when the wrapped handler itself calls a
Temporaless core or canonical ConnectRPC workflow boundary.

`TimerPendingError` and `EventPendingError` are not mapped to a Prefect paused
state. A Temporaless run remains `IN_PROGRESS` while the invoking Prefect run
observes an error. Sleeps and waits with explicit `PollOptions` can receive a
later scanner dispatch; manual event/dependency waits need their application
delivery/completion path to invoke the run. Use Prefect retries with the same
application workflow/run IDs for short redelivery, or call the canonical
ConnectRPC workflow from Prefect and treat `UNAVAILABLE` as pending. Do not
claim that the two run-state machines are one lifecycle.

## Dagster

There is no supported same-process Dagster adapter today. Dagster
[`1.13.14`](https://github.com/dagster-io/dagster/releases/tag/1.13.14) requires
`protobuf>=4,<7` on Python 3.11+, while Temporaless requires protobuf 7.35.1 or
newer. The official
[`1.13.14` package metadata](https://github.com/dagster-io/dagster/blob/1.13.14/python_modules/dagster/pyproject.toml)
and current Dagster main branch both retain that upper bound.

The production-safe integration is process isolation:

```text
Dagster process/environment (protobuf <7)
        │ generated application ConnectRPC client
        │ protobuf wire bytes
        ▼
Temporaless workflow service (protobuf >=7.35.1)
```

- Generate client and server bindings independently from the same application
  `.proto`.
- Keep Dagster assets, jobs, schedules, sensors, partitions, lineage, and UI
  in the Dagster process.
- Keep replay, activities, timers, claims, and object-store records in the
  Temporaless service.
- Put explicit workflow/run IDs in the application request or derive them
  deterministically from Dagster run metadata.
- Retry network/pending responses with the same IDs so terminal replay and
  activity checkpoints suppress duplicate side effects.
- Do not import Temporaless or its protobuf-7-generated Python modules in the
  Dagster interpreter, and do not let Dagster write Temporaless bucket records
  directly.

Same-process support can be reconsidered only after an official Dagster release
permits protobuf 7 and an exact-Git-SHA combined installation plus real
end-to-end job test passes.

## Temporal

`adapters/{go,py}/temporalcompat` delegates activity execution, retries,
timeouts, and durable sleep to the real Temporal SDK. It accepts the
Temporaless unary protobuf shape; it does not run existing arbitrary
Temporal-native workflows against Temporaless storage.

Temporal signals, queries, updates, child workflows, custom payload
converters, workflow-level retry semantics, and history behavior stay native
Temporal concerns. If an application needs those features, use a native
Temporal workflow rather than expecting an import substitution.

## Minimum Integration Proof

An orchestrator integration should not be called production-compatible until
tests prove:

- the exact locked framework SDK and orchestrator versions install together,
  or the processes are deliberately isolated;
- a real generated protobuf request crosses the actual runtime/transport
  boundary and returns the declared response type;
- explicit workflow/run/activity IDs are preserved across retries;
- a duplicate attempt replays without repeating an already completed activity
  side effect;
- pending, terminal application, and network failures have documented states
  on both sides;
- no orchestrator SDK dependency leaks into Temporaless core packages.
