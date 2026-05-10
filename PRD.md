# Product Decisions

This file is a queue of decisions that need user input before implementation can proceed. Each item is structured: **what is being decided**, **what is currently chosen**, **why it might change**, and **suggested options**.

When a decision is settled, move the chosen direction into `AGENTS.md` (or the relevant `docs/` file) and delete the entry here.

---

## D2. Long retry backoffs as durable timers

**Currently chosen:** All retry backoffs sleep in-process via `asyncio.sleep` (Python) / `time.Sleep` (Go).

This is fine for sub-second-to-second backoffs, but a 5-minute backoff on a serverless invocation wastes compute time and may exceed the host's request timeout.

**Why this might change:** LLM rate-limit retries are often 30s–10min. Stocks vendor 429 retries can be 5–60s. Keeping a serverless function alive through that is expensive.

**Concrete design when we ship it (partially landed):**
1. Add `RetryPolicy.durable_backoff_threshold = google.protobuf.Duration`. Zero (default) preserves current in-process behavior. *(Pending.)*
2. Add `ActivityStatus.ACTIVITY_STATUS_RETRYING`. *(Resolved by Iteration 21 — RETRYING records persist between in-process attempts; reused for the durable case.)*
3. Add `ActivityRecord.next_attempt_at = google.protobuf.Timestamp`. *(Pending.)*
4. In the retry loop: when the next interval `>= threshold`:
   - persist `ActivityRecord{status: RETRYING, attempts: [...so far], next_attempt_at: now+interval}` *(persistence resolved; the `next_attempt_at` field is the missing piece.)*
   - write a paired `TimerRecord` at `timers/activity-retry:{activity_id}:{attempt}.binpb` so the bundled timer scanner re-invokes the workflow after `fire_at`. *(Pending.)*
   - return `ErrTimerPending` from the activity so the workflow itself stays `IN_PROGRESS`. *(Pending.)*
5. On replay with `RETRYING` record: verify fingerprint, compare `now` vs `next_attempt_at`; if past, resume retry loop from `len(attempts)+1`; otherwise return `ErrTimerPending` again. *(Pending.)*

Implementation budget: ~80 LOC + tests in each language (smaller than originally estimated since RETRYING-record persistence already exists).

**Gating:** bring this in once a real LLM workflow hits 30s+ rate-limit windows in production. Until then, in-process retries are simpler and sufficient. The user has confirmed multiple times that this is gated and shouldn't be built preemptively.

---

## D5. Temporal drop-in adapter direction

**Currently:** `adapters/{go,py}/temporalcompat` runs Temporaless-shaped handlers on the **real Temporal SDK** (worker direction).

**The stated drop-in goal is the inverse arrow:** existing Temporal user code (`workflow.ExecuteActivity`, `workflow.Sleep`, `RetryPolicy`, etc.) running on **Temporaless storage** by changing only client/worker init.

**Why this needs a decision:** Building the inverse adapter is large (Temporal payload converter shim, signal/query model, history events). It's a different package entirely. The current `temporalcompat` is fine but should arguably be renamed `temporalworker` to free up the name for the drop-in shim.

**Options:**
- Rename current `temporalcompat` → `temporalworker`; reserve `temporaltemporaless` (or similar) for the inverse direction.
- Leave naming alone; build the inverse as `temporalcompat/v2`.
- De-prioritize the inverse drop-in until core is finished.

---

## D11. Dagster compat adapter — blocked on protobuf 7

**Currently:** No `adapters/py/dagstercompat`.

**Why blocked:** Dagster's latest releases (1.13.x) pin `protobuf>=4,<7`. Temporaless requires `protobuf>=7.34.1` (released 2026-03-20). The two cannot coexist in one Python process.

**Options when Dagster catches up:**

1. Build `adapters/py/dagstercompat` mirroring `prefectcompat` — wrap a Temporaless-shaped handler as a Dagster `@op` / `@asset`. ~120 LOC + tests, same shape as the Prefect adapter. Trigger: Dagster releases a version supporting `protobuf>=7`.
2. Cross-process integration via gRPC: Dagster process makes gRPC calls to a Temporaless service (no shared deps). Document this pattern in `docs/comparisons.md` rather than as a separate uv project.

**Decision:** wait for Dagster to support protobuf 7. The cross-process pattern is already implicitly supported (any gRPC client speaks to our `ConnectStore`/handler surface) and doesn't need a dedicated adapter.

---

## D10 follow-ups. Retention enhancements

**Resolved by Iteration 15:** `Store.Sweep` (RPC + Go/Python implementation) deletes COMPLETED runs older than `maxAge`. Caller decides cadence and threshold.

Future enhancements (defer until requested):
- Per-namespace or per-workflow-id retention overrides.
- Archival hook (copy to cold storage before delete).
- Separate retention class for FAILED records (forensics may want longer retention).
