"""Tests for the outbox idempotency-key helper.

Mirrors adapters/go/outbox/outbox_test.go.
"""

from __future__ import annotations

from temporaless.outbox import PREFIX, derive
from temporaless.storage import DEFAULT_NAMESPACE


def test_deterministic_over_identity():
    a = derive(DEFAULT_NAMESPACE, "wf-a", "run-1", "act:1")
    b = derive(DEFAULT_NAMESPACE, "wf-a", "run-1", "act:1")
    assert a == b


def test_different_identity_produces_different_key():
    cases = [
        ("default", "wf-a", "run-1", "act:1"),
        ("tenant-b", "wf-a", "run-1", "act:1"),
        ("default", "wf-b", "run-1", "act:1"),
        ("default", "wf-a", "run-2", "act:1"),
        ("default", "wf-a", "run-1", "act:2"),
    ]
    seen: set[str] = set()
    for namespace, wf, run, act in cases:
        key = derive(namespace, wf, run, act)
        assert key not in seen, f"collision for {namespace}/{wf}/{run}/{act}"
        seen.add(key)


def test_has_framework_prefix():
    key = derive(DEFAULT_NAMESPACE, "wf", "run", "act")
    assert key.startswith(PREFIX)


def test_stable_length():
    # PREFIX + 32 hex chars (16 bytes of SHA-256).
    got = derive(DEFAULT_NAMESPACE, "wf", "run", "act")
    assert len(got) == len(PREFIX) + 32


def test_long_inputs_still_fixed_width():
    long = "a" * 200
    short = derive(DEFAULT_NAMESPACE, "wf", "run", "act")
    full = derive(DEFAULT_NAMESPACE, long, long, long)
    assert len(short) == len(full)
