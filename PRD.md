# Product Decisions

This file is a queue of decisions that need user input before implementation can proceed. Each item is structured: **what is being decided**, **what is currently chosen**, **why it might change**, and **suggested options**.

When a decision is settled, move the chosen direction into `AGENTS.md` (or the relevant `docs/` file) and delete the entry here.

---

## D10 follow-ups. Retention enhancements

**Resolved by Iteration 15:** `Store.Sweep` (RPC + Go/Python implementation) deletes COMPLETED runs older than `maxAge`. Caller decides cadence and threshold.

Future enhancements (defer until requested):
- Per-namespace or per-workflow-id retention overrides.
- Archival hook (copy to cold storage before delete).
- Separate retention class for FAILED records (forensics may want longer retention).
