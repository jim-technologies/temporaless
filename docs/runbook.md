# Operator Runbook

What to do when things go wrong in production. Each entry: **symptom**, **diagnostic command**, **fix**. Pair with [`docs/production-checklist.md`](production-checklist.md) (pre-launch) and [`docs/hard-cases.md`](hard-cases.md) (known sharp edges).

The framework's design means most operator work is **reading records out of object storage and selectively deleting/resetting them**. There's no engine state to repair, no leader to evict, no quorum to fix.

---

## 1. Workflow stuck in `IN_PROGRESS` past expected duration

**Symptom:** A workflow you expect to be `COMPLETED` is still `IN_PROGRESS` long after its activities + timers + waits should have cleared.

**Diagnose:**

```python
from temporaless.inspector import list_in_flight_workflows
from temporaless_indexstore import IndexedStore
import opendal

store = IndexedStore.from_opendal(opendal.AsyncOperator("s3", root="..."), "/var/temporaless/index.sqlite")
for wf in await list_in_flight_workflows(store):
    if wf.workflow_id == "the-stuck-one":
        print(wf)  # check created_at, last activity_id, pending events
```

Inspect the actual records (the framework's transparency promise — they're protobuf at deterministic paths):

```sh
aws s3 ls s3://your-bucket/temporaless/v2/default/<wf_id>/<run_id>/
# Should show workflow.binpb plus activity/, timer/, event/, claim/ subdirs
```

**Common causes & fixes:**

- **Pending timer that never fired** — the timer scanner stopped ticking. Verify your scheduler/CronJob is alive; restart it. The next tick will pick up overdue timers.
- **Pending event that nothing's delivered** — find the event_id, deliver via `temporaless.send_event(store, key, payload)`.
- **Pending dependency** (`WorkflowDependencyPendingError` mapped to `Unavailable`) — the upstream workflow hasn't completed. Check it with the same procedure recursively.
- **Worker crashed mid-execution without workflow claims** — re-invoke the workflow. Existing activity records replay; the body resumes from the first not-yet-recorded boundary with at-least-once execution at the missing boundary.
- **Worker crashed while holding `workflow:execution`** — a create-only claim can remain after the process is gone. Confirm that no worker is still executing, then delete that exact key through `ClaimStore.delete_claim` / `DeleteClaim` before re-invoking. Its lease timestamp does not free it automatically.
- **Activity claim retained after an ambiguous exit** — cancellation may have arrived after an external side effect, or storage may have failed after the activity returned. Confirm the old invocation is gone and reconcile the external side effect before deleting `activity:{activity_id}`. If the activity record is terminal or `RETRYING` with its timer present, normal cleanup should have released the claim; a retained claim then indicates cleanup failure.

**If unrecoverable:** do not delete the parent workflow first. Follow the
quiesced child-first reset procedure below. `inspector.reset_workflow(store,
key)` deletes only the workflow record; it is not a recursive reset or an
execution fence.

## 1a. Retry only failed partitions or repair a durable retry

**Symptom:** A batch workflow has successful activity checkpoints plus a small
set of `FAILED` or `RETRYING` activities, and you want to preserve the
successful results.

First distinguish repair from reset:

- For a valid `RETRYING` activity, re-invoke the same run before deleting
  anything. Replay validates the stored retry policy and caller-supplied
  `retry_timer_id`. It recreates a missing or older paired retry timer from the
  persisted attempt schedule, and it honors a compatible newer timer left by
  a timer-first crash. Do not hand-edit the activity or invent a timer
  deadline; the prepared due record repairs an interrupted canonical write.
- For a terminal `FAILED` activity that should consume a new retry budget, use
  the reset sequence below. Also use it after preserving forensic copies of a
  genuinely corrupt or conflicting activity/timer pair.

The safe partial-reset order is:

1. Stop every trigger, timer scanner, and worker that can invoke this exact
   run. Confirm no workflow/activity claim owner is live; quiescence is part of
   the operation because point deletes are not transactional.
2. Read the failed `ActivityRecord`s and take their paired timer IDs only from
   `retry_timer_id`. Validate that any existing `TimerRecord` has
   `TIMER_KIND_ACTIVITY_RETRY` and the reciprocal `retry_activity_id`. Never
   guess an ID or delete a timer owned by another activity.
3. While the parent workflow is still terminal, delete each validated paired
   retry timer through `Store.delete_timer` / `Store.DeleteTimer`, then delete
   only the failed activity records through `reset_activity` /
   `inspector.ResetActivity`. Leave completed activity records intact; they are
   the successful checkpoints.
4. Delete the parent workflow record **last** with `reset_workflow` /
   `inspector.ResetWorkflow`. Remove only claims whose former owners you have
   proved are gone.
5. Invoke the run once, then re-enable normal scanner/trigger delivery.

`DeleteTimer` writes a durable `CANCELED` prepared tombstone before removing the
canonical retry timer, so its former wake is suppressed even after the parent
workflow is reset. Keep using the public delete operation rather than removing
the canonical object directly. Execution claims suppress cooperating overlap;
activity idempotency is still required.

---

## 2. Spike in `ClaimBusy` errors (Code.ALREADY_EXISTS)

**Symptom:** RPC clients see a sustained burst of `Code.ALREADY_EXISTS` from one or more workflow IDs. RecordStoreService logs show repeated claim conflicts.

**Diagnose:**

```python
from temporaless.storage import ClaimKey

key = ClaimKey(
    workflow_id="prices:AAPL",
    run_id="2026-05-04",
    claim_id="workflow:execution",  # or activity:<activity_id>
)
claim = await store.get_claim(key)
if claim is not None:
    print(claim.owner_id, claim.lease_expires_at)
```

**Common causes:**

- **Claim leak.** A worker died after acquiring a create-only claim but before releasing it. `lease_expires_at` is diagnostic metadata, not permission to take over: waiting past it does not free the claim. Confirm that the old worker is gone, then delete the activity claim or per-run `workflow:execution` claim explicitly through `ClaimStore.delete_claim` / `DeleteClaim`. CAS takeover is not implemented by the current core.
- **Thundering-herd retries.** Many clients retrying the same workflow in lockstep. Add jittered backoff client-side; the server is doing the right thing rejecting duplicate work.
- **Wrong claim store backend.** OpenDAL `fs` is **not multi-process safe**. If you see this in dev with multiple worker pods sharing an emptyDir, switch to a real cloud backend (`gocloud.dev` blob with `IfNotExist`, S3 conditional puts, GCS `ifGenerationMatch`).
- **Claim capability is unavailable.** If a trigger opts into `claim_owner_id` or `concurrency_key` but the remote RecordStoreService has no claim backend, the runtime returns failed-precondition before writing `IN_PROGRESS`. Configure a create-only claim store or remove the coordination option and accept at-least-once overlap.

---

## 2a. Concurrency capacity remains consumed after a crash

**Symptom:** Calls receive `ConcurrencyBusyError` / `RESOURCE_EXHAUSTED` even though fewer than `concurrency_limit` workflows are live.

Concurrency slots are claims under the framework scope `workflow_id=__concurrency__`, `run_id=<concurrency_key>`, with claim IDs `slot:0`, `slot:1`, and so on. They are intentionally outside any one application run because many workflows share the pool. Consequently, application `DeleteRun` cannot discover or delete them.

Inspect that pool in the claim backend and compare each slot's caller-supplied `owner_id` with your invocation/worker logs. After proving the owner is gone, delete that exact `ClaimKey` through `ClaimStore.delete_claim` / `DeleteClaim`; do not treat `lease_expires_at` as automatic permission. If owner IDs are reused across workers, use deployment logs and shutdown state to disambiguate before cleanup.

---

## 3. Storage backend outage (S3 / GCS / Azure)

**Symptom:** All RPCs fail with 5xx. Application logs show OpenDAL errors.

**Diagnose:**

```sh
# Confirm the backend is the issue, not your application
aws s3 ls s3://your-bucket/  # or gsutil ls / az storage blob list
```

**Recovery:**

1. **A transient outage needs no record mutation.** When the backend recovers,
   callers or queues must redeliver failed invocations. Nonterminal workflows
   stay `IN_PROGRESS`; for scheduled waits, tick the scanner and let the
   application-supplied dispatcher re-invoke each due run. The scanner does not
   resume event-waiting or otherwise unscheduled workflows by itself.
2. **If the bucket itself is gone:** restore a coherent point-in-time snapshot
   from cross-region replication or object versioning. Workflow, activity, and
   timer protobufs are overwritten as their state advances, and each timer has
   a paired `_due` write-ahead record. Do not combine arbitrary object versions
   from different times.
3. **If one record is corrupt:** quiesce writers/scanners, preserve the bytes for
   diagnosis, and restore a known-good object version when possible. Do not
   blindly delete an authoritative terminal/checkpoint record. Reset an
   activity together with its retry timer; reset a workflow only after applying
   the documented child-first reset procedure; repair a timer through the
   upgraded `PutTimer` / `put_timer` API so its canonical and `_due` records are
   republished together.

---

## 4. Sustained 401s after a deploy

**Symptom:** Auth interceptor logs `auth.token_mismatch` for legitimate clients.

**Cause:** Token was rotated server-side but clients still have the old token cached.

**Fix:**

- Confirm via your secret manager (Vault / SSM / Secret Manager / env var) that the server-side token matches what clients have.
- Rotate clients to the new token (push via your config-management tool).
- For graceful rotation in the future: support two valid tokens during a
  window. Adapt `BearerTokenAuth` in `examples/py/production_server.py` and
  `bearerTokenAuth` in `examples/go/production-server/main.go` to accept a
  list:

  ```python
  import hmac

  if not any(
      hmac.compare_digest(authz, f"Bearer {token}")
      for token in self._valid_tokens
  ):
      raise ConnectError(Code.UNAUTHENTICATED, ...)
  ```

  Issue both tokens (current + previous) for the rotation window, rotate clients, then drop the old token.

---

## 5. Workflow re-running its activity body on every call

**Symptom:** A workflow that used to replay cleanly now re-executes its activity on every invocation. Vendor calls multiply; idempotency feels broken.

**Cause:** `code_version` on the workflow `Options` changed. Activities replay on `(workflow_id, run_id, activity_id, code_version)`; bumping `code_version` invalidates all stored records as a deliberate design choice.

**Fix:**

- **If the change was intentional** (you deployed a body change that's incompatible with old records): no action — the framework is doing the right thing.
- **If the change was accidental** (you mistyped a version, or your deploy tool injected a new SHA without you meaning it to): roll back the code_version, OR delete the affected runs (`store.delete_workflow(key)`) so the new code can re-record from scratch.

---

## 6. Backfill keeps producing PENDING entries

**Symptom:** Calling `temporaless.backfill.backfill(...)` reports many entries as `PENDING`, not `SUCCEEDED` or `FAILED`.

**Cause:** Workflow bodies are returning `TimerPendingError` / `EventPendingError` / `WorkflowDependencyPendingError`. They're stuck waiting for something (a sleep to elapse, an event to arrive, an upstream workflow to finish).

**Fix:**

- For sleeps: ensure a timer-scanner tick is calling `due_timers` / `DueTimers`
  and dispatching every returned wake to the application workflow handler.
  The ConnectStore production server does not do this itself.
- For events: trace the workflow body to find the event_id it's waiting on, deliver it.
- For upstream deps: backfill the upstream pipeline first, then re-run the dependent backfill. Backfill is idempotent — re-running the same set is free for already-`SUCCEEDED` entries.

```python
report = await backfill(invoke, run_ids, concurrency=10)
print(f"pending: {[e.run_id for e in report.pending()]}")
print(f"failed:  {[e.run_id for e in report.failed()]}")
# Once you've cleared the upstream dep, re-run:
report2 = await backfill(invoke, run_ids, concurrency=10)
```

---

## 7. Process fails health check, supervisor restart loop

**Symptom:** Your supervisor (Lambda runtime / Cloud Run / systemd / K8s / ...) keeps restarting the process. Health probe returns non-200 or times out.

**Diagnose:**

Read the previous-instance logs (Lambda → CloudWatch, Cloud Run → Logging, systemd → `journalctl -u <unit>`, etc.). The process emits structured JSON to stdout; the last few lines before the restart usually have the cause.

**Common causes:**

- **Required configuration or storage initialization failed at startup.** The
  production examples exit before listening when auth/storage configuration is
  missing, malformed, or unsafe (`fs` without explicit acknowledgement), or
  when OpenDAL construction fails. Fix the startup log's `config.*` /
  `storage.init_failed` error and redeploy. Once the server is listening,
  `/healthz` is liveness-only and does not probe the backend.
- **Out-of-memory.** Per-RPC overhead is small but per-workflow with large protobuf messages can grow. Bump the platform's memory limit (Lambda memory, Cloud Run memory, container limits).
- **Stuck event loop.** Rare; usually indicates a sync function called from an async context. The framework rejects sync workflow bodies at wrap time, but an activity body that calls a blocking C extension can still cause this. Profile with `py-spy dump --pid <pid>` to see the stuck frame.

---

## 8. Disaster recovery: bucket gone

**Severity:** Catastrophic. The framework has no off-bucket state — losing the bucket loses all workflow history.

**Recovery:**

1. **Restore from cross-region replication** if you set it up (you should have — see `docs/production-checklist.md`). Point the OpenDAL operator at the replica until the primary is back.
2. **Restore from versioning** if the loss was a delete, not a region-wide outage. S3 versioning lets you `s3api list-object-versions` and recover deleted objects.
3. **Replay from upstream sources.** If you can't recover the bucket, you'll lose the workflow record state but your activity bodies' *side effects* (writes to your database, vendor API calls) are still there. New workflow runs with fresh `run_id`s will record their own state going forward.

The framework deliberately makes this DR scenario boring — there's no separate database to back up, no quorum to restore. Your bucket backup IS the framework's backup.

---

## 9. CronJob double-firing schedules

**Symptom:** A scheduled workflow runs twice for the same fire time.

**Cause:** Two cron-scheduler instances ticked at the same time and both dispatched. A terminal run replays, but two first invocations can overlap unless the dispatched workflow sets `claim_owner_id` and uses an atomic claim store. In that mode one invocation runs and the other receives `ClaimBusyError`; without it, treat dispatch as at-least-once and keep activities idempotent.

If you want to suppress duplicate dispatches after restarts (to save the workflow-run round-trip), seed the scheduler with `cronscheduler.LastFireFromRuns` / `last_fire_from_runs`. It reads the latest-run pointer for each schedule and both schedulers converge on the same last-fire state.

---

## See also

- `docs/hard-cases.md` — known sharp edges (concurrency, retries, backend atomicity)
- `docs/production-checklist.md` — pre-launch list
- `docs/deployment.md` — architectural choices
- `docs/scheduling.md` — timers + cron + scanner deep dive
