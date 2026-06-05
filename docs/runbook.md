# Operator Runbook

What to do when things go wrong in production. Each entry: **symptom**, **diagnostic command**, **fix**. Pair with [`docs/production-checklist.md`](production-checklist.md) (pre-launch) and [`docs/hard-cases.md`](hard-cases.md) (known sharp edges).

The framework's design means most operator work is **reading records out of object storage and selectively deleting/resetting them**. There's no engine state to repair, no leader to evict, no quorum to fix.

---

## 1. Workflow stuck in `IN_PROGRESS` past expected duration

**Symptom:** A workflow you expect to be `COMPLETED` is still `IN_PROGRESS` long after its activities + timers + waits should have cleared.

**Diagnose:**

```python
from temporaless.inspector import list_in_flight_workflows
from temporaless import OpenDALStore
import opendal

store = OpenDALStore(opendal.AsyncOperator("s3", root="..."))
async for wf in list_in_flight_workflows(store):
    if wf.workflow_id == "the-stuck-one":
        print(wf)  # check created_at, last activity_id, pending events
```

Inspect the actual records (the framework's transparency promise — they're protobuf at deterministic paths):

```sh
aws s3 ls s3://your-bucket/temporaless/v1/namespace=default/workflow_id=<wf_id>/run_id=<run_id>/
# Should show kind=workflow/, kind=activity/, kind=timer/, kind=event/, kind=claim/ subdirs
```

**Common causes & fixes:**

- **Pending timer that never fired** — the timer scanner stopped ticking. Verify your scheduler/CronJob is alive; restart it. The next tick will pick up overdue timers.
- **Pending event that nothing's delivered** — find the event_id, deliver via `temporaless.send_event(store, key, payload)`.
- **Pending dependency** (`WorkflowDependencyPendingError` mapped to `Unavailable`) — the upstream workflow hasn't completed. Check it with the same procedure recursively.
- **Worker crashed mid-execution** — re-invoke the workflow (`workflow.run` is idempotent on the same input). Existing activity records replay; the body resumes from the first not-yet-recorded boundary.

**If unrecoverable:** `inspector.reset_workflow(store, key)` deletes the workflow record (and optionally child records) so the next invocation starts fresh.

---

## 2. Spike in `ClaimBusy` errors (Code.ALREADY_EXISTS)

**Symptom:** RPC clients see a sustained burst of `Code.ALREADY_EXISTS` from one or more workflow IDs. RecordStoreService logs show repeated claim conflicts.

**Diagnose:**

```python
from temporaless.inspector import list_claims_for
async for claim in list_claims_for(store, workflow_id="prices:AAPL", run_id="2026-05-04"):
    print(claim.claim_id, claim.owner_id, claim.lease_expires_at)
```

**Common causes:**

- **Claim leak.** A worker died after acquiring a claim but before releasing it; the lease is alive until expiry. Wait `DEFAULT_CLAIM_LEASE_DURATION` (15 min default) and retry; or call `inspector.reset_activity(store, key)` to clear the claim explicitly.
- **Thundering-herd retries.** Many clients retrying the same workflow in lockstep. Add jittered backoff client-side; the server is doing the right thing rejecting duplicate work.
- **Wrong claim store backend.** OpenDAL `fs` is **not multi-process safe**. If you see this in dev with multiple worker pods sharing an emptyDir, switch to a real cloud backend (`gocloud.dev` blob with `IfNotExist`, S3 conditional puts, GCS `ifGenerationMatch`).

---

## 3. Storage backend outage (S3 / GCS / Azure)

**Symptom:** All RPCs fail with 5xx. Application logs show OpenDAL errors.

**Diagnose:**

```sh
# Confirm the backend is the issue, not your application
aws s3 ls s3://your-bucket/  # or gsutil ls / az storage blob list
```

**Recovery:**

1. **The framework needs no manual intervention.** When the backend recovers, in-flight requests retry naturally; pending workflows stay `IN_PROGRESS` and resume on the next scanner tick.
2. **If the bucket itself is gone:** restore from cross-region replication / versioning. The framework's records are append-only protobuf binaries — versioning gives you point-in-time recovery without bespoke logic.
3. **If a record was corrupted:** delete the corrupted record's path; the next workflow invocation re-creates it (replay re-executes the boundary). Use `protoc --decode` to inspect a record before deleting if you're not sure it's actually corrupt.

---

## 4. Sustained 401s after a deploy

**Symptom:** Auth interceptor logs `auth.token_mismatch` for legitimate clients.

**Cause:** Token was rotated server-side but clients still have the old token cached.

**Fix:**

- Confirm via your secret manager (Vault / SSM / Secret Manager / env var) that the server-side token matches what clients have.
- Rotate clients to the new token (push via your config-management tool).
- For graceful rotation in the future: support two valid tokens during a window. Update `BearerTokenAuth` in `examples/{go,py}/production_server.py` to accept a list:

  ```python
  if authz[len("Bearer "):] not in self._valid_tokens:
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

- For sleeps: ensure the timer-scanner is running (`scheduler.tick()` invoked by your CronJob).
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

- **Storage backend init failed at startup.** OpenDAL operator construction errored (bad credentials, region misconfigured). The process was alive but `/healthz` returned 500. Fix the config, redeploy.
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

**Cause:** Two cron-scheduler instances ticked at the same time and both dispatched. The framework's `workflow.run` is idempotent — the second dispatch will see the existing record and short-circuit via replay. **No correctness issue.**

If you want to suppress the duplicate dispatch entirely (to save the storage round-trip), set `cronscheduler.LastFireFromRuns` to derive last-fire from existing run records — both schedulers see the same state and both correctly skip the already-fired tick.

---

## See also

- `docs/hard-cases.md` — known sharp edges (concurrency, retries, backend atomicity)
- `docs/production-checklist.md` — pre-launch list
- `docs/deployment.md` — architectural choices
- `docs/scheduling.md` — timers + cron + scanner deep dive
