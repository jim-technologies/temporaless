#!/usr/bin/env python3
"""Apply the narrow v0.9 code-version deletion exception to Buf FILE checks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from typing import Any


_V090_ALLOWED_DELETIONS = frozenset(
    {
        (
            "WorkflowOptions",
            "3",
        ),
        (
            "ActivityRecord",
            "4",
        ),
        (
            "WorkflowRecord",
            "4",
        ),
        (
            "TimerRecord",
            "4",
        ),
        (
            "ClaimRecord",
            "6",
        ),
    }
)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--against", required=True)
    parser.add_argument("--baseline-tag", required=True)
    parser.add_argument("--version", required=True)
    return parser.parse_args()


def _allowed_v090_deletion(violation: dict[str, Any]) -> bool:
    if violation.get("path") != "api/temporaless/v1/temporaless.proto":
        return False
    if violation.get("type") != "FIELD_NO_DELETE":
        return False

    message = violation.get("message")
    if not isinstance(message, str):
        return False
    for message_name, field_number in _V090_ALLOWED_DELETIONS:
        expected = (
            f'Previously present field "{field_number}" with name "code_version" '
            f'on message "{message_name}" was deleted.'
        )
        if message == expected:
            return True
    return False


def main() -> int:
    args = _args()
    result = subprocess.run(
        [
            "buf",
            "breaking",
            "api",
            "--against",
            args.against,
            "--error-format=json",
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return 0

    violations: list[dict[str, Any]] = []
    try:
        for line in result.stdout.splitlines():
            if line:
                violations.append(json.loads(line))
    except (json.JSONDecodeError, TypeError):
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.returncode

    intentional_transition = args.baseline_tag == "v0.8.2" and args.version == "0.9.0"
    unexpected = [
        violation
        for violation in violations
        if not intentional_transition or not _allowed_v090_deletion(violation)
    ]
    if unexpected or not violations:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        return result.returncode

    print(
        "Allowing the five reserved code_version field deletions for the "
        "intentional pre-1.0 v0.8.2 -> v0.9.0 transition."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
