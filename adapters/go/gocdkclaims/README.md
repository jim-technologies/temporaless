# GoCDK Claims Adapter

This is a decision adapter, not a Temporal compatibility adapter.

## Purpose

The adapter exists only because GoCDK Blob exposes `WriterOptions.IfNotExist`, which maps to native object-store create-if-absent behavior in supported drivers.

OpenDAL remains the default storage layer for workflow, activity, and timer records.

## Capability

`CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`

The adapter can create a claim object atomically when the object does not exist, perform ordinary idempotent deletion on orderly release, and list claims under one run for `DeleteRun` and claim-aware retention sweeps. It cannot refresh, conditionally release, or take over stale claims with compare-and-swap.

A process-level mutex serializes `TryCreateClaim` calls within a single Store instance. This compensates for GoCDK fileblob's `IfNotExist` being implemented as Stat-then-Rename (racy across goroutines). For multi-process or distributed atomicity, rely on the underlying driver's native preconditions: S3 `If-None-Match`, GCS `ifGenerationMatch=0`.

## Core Behavior

With `claim_owner_id` enabled, Temporaless uses this adapter for the per-run
`workflow:execution` claim and activity claims. Terminal workflow and activity
records replay first. Otherwise an existing workflow execution claim returns a
typed busy error even for the same owner; activity claims follow the same rule.
Terminal and fully persisted retry boundaries release activity claims. An
existing create-only claim also
remains busy after its lease timestamp because expiry does not prove takeover
is safe.

Manual cleanup or a future CAS-capable adapter is required for stale claim recovery.

## Rejected Behavior

- no check-then-write locking
- no Redis or database lock server
- no stale claim takeover without generation, ETag, or equivalent CAS support
