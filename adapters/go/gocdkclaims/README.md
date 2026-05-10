# GoCDK Claims Adapter

This is a decision adapter, not a Temporal compatibility adapter.

## Purpose

The adapter exists only because GoCDK Blob exposes `WriterOptions.IfNotExist`, which maps to native object-store create-if-absent behavior in supported drivers.

OpenDAL remains the default storage layer for workflow, activity, and timer records.

## Capability

`CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS`

The adapter can create a claim object atomically when the object does not exist. It cannot refresh, release, or take over stale claims with compare-and-swap.

A process-level mutex serializes `TryCreateClaim` calls within a single Store instance. This compensates for GoCDK fileblob's `IfNotExist` being implemented as Stat-then-Rename (racy across goroutines). For multi-process or distributed atomicity, rely on the underlying driver's native preconditions: S3 `If-None-Match`, GCS `ifGenerationMatch=0`.

## Core Behavior

When this adapter reports an existing claim and no completed activity record exists, Temporaless returns a typed busy error. It does this even when the claim lease timestamp is expired, because create-only storage does not prove that takeover is safe.

Manual cleanup or a future CAS-capable adapter is required for stale claim recovery.

## Rejected Behavior

- no check-then-write locking
- no Redis or database lock server
- no stale claim takeover without generation, ETag, or equivalent CAS support
