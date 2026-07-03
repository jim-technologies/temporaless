"""One-shot v1 hive-layout to v2 flat-key migration tool.

This module is intentionally the only Python core code that parses old
``temporaless/v1/.../key=value/...`` paths. Runtime storage constructs v2 keys
from protobuf keys and never inverts paths back into identities.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import AsyncIterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import opendal
from google.protobuf.message import DecodeError
from protovalidate import ValidationError

from temporaless.storage import (
    ClaimKey,
    OpenDALStore,
    Store,
    activity_key_from_proto,
    claim_key_from_proto,
    event_key_from_proto,
    timer_key_from_proto,
    workflow_key_from_proto,
)
from temporaless.v1 import temporaless_pb2

_V1_ROOT = "temporaless/v1/"
_DEFAULT_RUN_ID_FORMATS = (
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d",
    "%Y%m%dT%H%M%SZ",
    "%Y%m%dT%H%M%S",
    "%Y%m%d",
)


@dataclass(frozen=True)
class _SourceRecord:
    kind: str
    path: str
    record: Any
    namespace: str = ""
    workflow_id: str = ""
    run_id: str = ""
    read_error: str = ""

    @property
    def run_tuple(self) -> tuple[str, str, str]:
        if self.record is None:
            return (self.namespace or "default", self.workflow_id, self.run_id)
        key = self.record.key
        return (key.namespace or "default", key.workflow_id, key.run_id)


@dataclass(frozen=True)
class _V1Path:
    kind: str
    namespace: str
    workflow_id: str
    run_id: str


@dataclass
class _Counts:
    read: int = 0
    migrated: int = 0
    skipped: int = 0


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    asyncio.run(_run(args))


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rewrite Temporaless v1 hive-layout protobuf records to v2 flat keys."
    )
    parser.add_argument(
        "--source-fs-root", required=True, help="Filesystem root containing temporaless/v1"
    )
    parser.add_argument(
        "--dest-fs-root", required=True, help="Filesystem root to receive temporaless/v2"
    )
    parser.add_argument("--audit-log", required=True, help="JSONL audit log path")
    parser.add_argument(
        "--index-sqlite",
        help="Optional SQLite path for a write-through RecordQueryService index.",
    )
    parser.add_argument(
        "--run-id-format",
        action="append",
        default=[],
        help=(
            "strftime/strptime format used to compare schedule run_ids in "
            "--newest-per-workflow mode. May be repeated; common ISO-like "
            "formats are tried by default."
        ),
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--all-runs",
        action="store_true",
        help="Migrate every v1 run. This is the default.",
    )
    mode.add_argument(
        "--newest-per-workflow",
        action="store_true",
        help="Migrate only the newest run for each (namespace, workflow_id).",
    )
    return parser.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    source = opendal.AsyncOperator("fs", root=args.source_fs_root)
    dest_operator = opendal.AsyncOperator("fs", root=args.dest_fs_root)
    run_id_formats = tuple(args.run_id_format) or _DEFAULT_RUN_ID_FORMATS
    dest_store: Store = OpenDALStore(dest_operator, latest_run_id_formats=run_id_formats)
    if args.index_sqlite:
        try:
            from temporaless_indexstore import IndexedStore
        except ImportError as exc:  # pragma: no cover - depends on optional install
            raise SystemExit(
                "--index-sqlite requires the temporaless-indexstore adapter package"
            ) from exc

        dest_store = IndexedStore(dest_store, args.index_sqlite, operator=dest_operator)

    records = [record async for record in _read_v1_records(source)]
    selected_runs = _selected_runs(
        records,
        newest_per_workflow=args.newest_per_workflow,
        run_id_formats=run_id_formats,
    )

    audit_path = Path(args.audit_log)
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    counts = _Counts(read=len(records))
    with audit_path.open("w", encoding="utf-8") as audit:
        for source_record in records:
            selected = source_record.run_tuple in selected_runs
            reason = ""
            if source_record.read_error:
                dest_path = ""
                selected = False
                reason = source_record.read_error
                counts.skipped += 1
            elif selected:
                try:
                    dest_path = await _write_v2_record(dest_operator, dest_store, source_record)
                except (ValueError, ValidationError) as exc:
                    dest_path = ""
                    selected = False
                    reason = str(exc)
                    counts.skipped += 1
                else:
                    counts.migrated += 1
            else:
                dest_path = ""
                reason = "run not selected"
                counts.skipped += 1
            audit.write(
                json.dumps(
                    {
                        "source_path": source_record.path,
                        "dest_path": dest_path,
                        "kind": source_record.kind,
                        "namespace": source_record.run_tuple[0],
                        "workflow_id": source_record.run_tuple[1],
                        "run_id": source_record.run_tuple[2],
                        "migrated": selected,
                        "reason": reason,
                    },
                    sort_keys=True,
                )
                + "\n"
            )

    print(
        json.dumps(
            {"read": counts.read, "migrated": counts.migrated, "skipped": counts.skipped},
            sort_keys=True,
        ),
        file=sys.stdout,
    )


async def _read_v1_records(operator: opendal.AsyncOperator) -> AsyncIterable[_SourceRecord]:
    async for path in _walk_binpb(operator, _V1_ROOT):
        parsed_path = _parse_v1_path(path)
        if parsed_path is None:
            continue
        factory = {
            "workflow": temporaless_pb2.WorkflowRecord,
            "activity": temporaless_pb2.ActivityRecord,
            "timer": temporaless_pb2.TimerRecord,
            "event": temporaless_pb2.EventRecord,
            "claim": temporaless_pb2.ClaimRecord,
        }[parsed_path.kind]
        data = bytes(await operator.read(path))
        record = factory()
        try:
            record.ParseFromString(data)
        except DecodeError as exc:
            yield _SourceRecord(
                kind=parsed_path.kind,
                path=path,
                record=None,
                namespace=parsed_path.namespace,
                workflow_id=parsed_path.workflow_id,
                run_id=parsed_path.run_id,
                read_error=f"DecodeError: {exc}",
            )
            continue
        yield _SourceRecord(kind=parsed_path.kind, path=path, record=record)


def _parse_v1_path(path: str) -> _V1Path | None:
    if not path.startswith(_V1_ROOT) or not path.endswith("/record.binpb"):
        return None
    segments = path.removeprefix(_V1_ROOT).split("/")
    parsed: dict[str, str] = {}
    for segment in segments[:-1]:
        if "=" not in segment:
            return None
        key, value = segment.split("=", 1)
        parsed[key] = value
    kind = parsed.get("kind")
    if kind in {"workflow", "activity", "timer", "event", "claim"}:
        namespace = parsed.get("namespace")
        workflow_id = parsed.get("workflow_id")
        run_id = parsed.get("run_id")
        if namespace and workflow_id and run_id:
            return _V1Path(
                kind=kind,
                namespace=namespace,
                workflow_id=workflow_id,
                run_id=run_id,
            )
    return None


def _selected_runs(
    records: list[_SourceRecord],
    *,
    newest_per_workflow: bool,
    run_id_formats: tuple[str, ...] = _DEFAULT_RUN_ID_FORMATS,
) -> set[tuple[str, str, str]]:
    if not newest_per_workflow:
        return {record.run_tuple for record in records}

    newest: dict[tuple[str, str], _SourceRecord] = {}
    for source_record in records:
        if source_record.kind != "workflow" or source_record.record is None:
            continue
        namespace, workflow_id, run_id = source_record.run_tuple
        key = (namespace, workflow_id)
        if key not in newest or _is_newer_workflow_record(
            source_record, newest[key], run_id_formats
        ):
            newest[key] = source_record
    return {record.run_tuple for record in newest.values()}


async def _write_v2_record(
    dest_operator: opendal.AsyncOperator,
    dest_store: Store,
    source_record: _SourceRecord,
) -> str:
    record = source_record.record
    if record is None:
        raise ValueError("source record could not be decoded")
    if source_record.kind == "workflow":
        key = workflow_key_from_proto(record.key)
        await dest_store.put_workflow(record)
        return key.path()
    if source_record.kind == "activity":
        key = activity_key_from_proto(record.key)
        await dest_store.put_activity(record)
        return key.path()
    if source_record.kind == "timer":
        key = timer_key_from_proto(record.key)
        await dest_store.put_timer(record)
        return key.path()
    if source_record.kind == "event":
        key = event_key_from_proto(record.key)
        await dest_store.put_event(record)
        return key.path()
    if source_record.kind == "claim":
        key = claim_key_from_proto(record.key)
        await _write_claim_record(dest_operator, key, record)
        return key.path()
    raise ValueError(f"unsupported v1 record kind {source_record.kind!r}")


async def _write_claim_record(
    operator: opendal.AsyncOperator, key: ClaimKey, record: temporaless_pb2.ClaimRecord
) -> None:
    await operator.create_dir(key.dir_path())
    await operator.write(key.path(), record.SerializeToString(deterministic=True))


def _workflow_record_time(record: temporaless_pb2.WorkflowRecord) -> datetime:
    if record.HasField("completed_at"):
        return record.completed_at.ToDatetime().replace(tzinfo=UTC)
    if record.HasField("created_at"):
        return record.created_at.ToDatetime().replace(tzinfo=UTC)
    return datetime.min.replace(tzinfo=UTC)


def _is_newer_workflow_record(
    candidate: _SourceRecord, current: _SourceRecord, run_id_formats: tuple[str, ...]
) -> bool:
    candidate_fire_time = _parse_run_id_fire_time(candidate.run_tuple[2], run_id_formats)
    current_fire_time = _parse_run_id_fire_time(current.run_tuple[2], run_id_formats)
    if candidate_fire_time is not None and current_fire_time is not None:
        if candidate_fire_time != current_fire_time:
            return candidate_fire_time > current_fire_time
        return _workflow_record_time(candidate.record) >= _workflow_record_time(current.record)
    candidate_time = _workflow_record_time(candidate.record)
    current_time = _workflow_record_time(current.record)
    if candidate_time != current_time:
        return candidate_time > current_time
    return candidate.run_tuple[2] > current.run_tuple[2]


def _parse_run_id_fire_time(run_id: str, run_id_formats: tuple[str, ...]) -> datetime | None:
    for run_id_format in run_id_formats:
        if run_id_format == "%Y%m%d" and (len(run_id) != 8 or not run_id.isdigit()):
            continue
        try:
            parsed = datetime.strptime(run_id, run_id_format)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    return None


async def _walk_binpb(operator: opendal.AsyncOperator, root: str) -> AsyncIterable[str]:
    queue = [root]
    while queue:
        current = queue.pop(0)
        try:
            entries = sorted(
                [entry async for entry in await operator.list(current)], key=lambda e: e.path
            )
        except opendal.exceptions.NotFound:
            continue
        for entry in entries:
            path = entry.path
            if path == current:
                continue
            if path.endswith("/"):
                queue.append(path)
            elif path.endswith(".binpb"):
                yield path


if __name__ == "__main__":
    main()
