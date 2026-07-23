import json
from datetime import UTC, datetime
from types import SimpleNamespace

import opendal
from google.protobuf.timestamp_pb2 import Timestamp

from temporaless.migrate_v1_to_v2 import (
    _parse_run_id_fire_time,
    _run,
    _selected_runs,
    _SourceRecord,
)
from temporaless.storage import WORKFLOW_RECORD_SCHEMA_VERSION, OpenDALStore, WorkflowKey
from temporaless.v1 import temporaless_pb2


def test_newest_per_workflow_selects_by_parsed_fire_time_before_write_time() -> None:
    records = [
        _SourceRecord(
            kind="workflow",
            path="old-backfill",
            record=_workflow_record(
                "prices:aapl",
                "2020-01-01",
                completed_at=datetime(2026, 7, 3, tzinfo=UTC),
            ),
        ),
        _SourceRecord(
            kind="workflow",
            path="newer-fire",
            record=_workflow_record(
                "prices:aapl",
                "2026-07-01",
                completed_at=datetime(2026, 7, 1, tzinfo=UTC),
            ),
        ),
    ]

    selected = _selected_runs(
        records,
        newest_per_workflow=True,
        run_id_formats=("%Y-%m-%d",),
    )

    assert selected == {("default", "prices:aapl", "2026-07-01")}


async def test_migration_skips_invalid_legacy_ids_and_continues(tmp_path, capsys) -> None:
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"
    audit_log = tmp_path / "audit.jsonl"
    source = opendal.AsyncOperator("fs", root=str(source_root))
    await _write_v1_workflow(source, _workflow_record("_old", "r1", completed_at=datetime.now(UTC)))
    await _write_v1_workflow(source, _workflow_record("good", "r1", completed_at=datetime.now(UTC)))

    await _run(
        SimpleNamespace(
            source_fs_root=str(source_root),
            dest_fs_root=str(dest_root),
            audit_log=str(audit_log),
            index_sqlite=None,
            run_id_format=[],
            newest_per_workflow=False,
        )
    )

    summary = json.loads(capsys.readouterr().out)
    audit = [json.loads(line) for line in audit_log.read_text(encoding="utf-8").splitlines()]
    dest = OpenDALStore(opendal.AsyncOperator("fs", root=str(dest_root)))

    assert summary == {"read": 2, "migrated": 1, "skipped": 1}
    assert {row["workflow_id"]: row["migrated"] for row in audit} == {"_old": False, "good": True}
    assert next(row["reason"] for row in audit if row["workflow_id"] == "_old")
    assert await dest.get_workflow(WorkflowKey(workflow_id="good", run_id="r1")) is not None


async def test_migration_skips_corrupt_v1_blob_and_continues(tmp_path, capsys) -> None:
    source_root = tmp_path / "source"
    dest_root = tmp_path / "dest"
    audit_log = tmp_path / "audit.jsonl"
    source = opendal.AsyncOperator("fs", root=str(source_root))
    await _write_v1_blob(source, "default", "corrupt", "r1", b"not a workflow record")
    await _write_v1_workflow(source, _workflow_record("good", "r1", completed_at=datetime.now(UTC)))

    await _run(
        SimpleNamespace(
            source_fs_root=str(source_root),
            dest_fs_root=str(dest_root),
            audit_log=str(audit_log),
            index_sqlite=None,
            run_id_format=[],
            newest_per_workflow=False,
        )
    )

    summary = json.loads(capsys.readouterr().out)
    audit = [json.loads(line) for line in audit_log.read_text(encoding="utf-8").splitlines()]
    dest = OpenDALStore(opendal.AsyncOperator("fs", root=str(dest_root)))

    assert summary == {"read": 2, "migrated": 1, "skipped": 1}
    assert {row["workflow_id"]: row["migrated"] for row in audit} == {
        "corrupt": False,
        "good": True,
    }
    assert "DecodeError" in next(row["reason"] for row in audit if row["workflow_id"] == "corrupt")
    assert await dest.get_workflow(WorkflowKey(workflow_id="good", run_id="r1")) is not None


def test_migration_compact_date_run_ids_require_exact_eight_digits() -> None:
    assert _parse_run_id_fire_time("100056", ("%Y%m%d",)) is None
    assert _parse_run_id_fire_time("202661", ("%Y%m%d",)) is None
    assert _parse_run_id_fire_time("20260601", ("%Y%m%d",)) == datetime(2026, 6, 1, tzinfo=UTC)


async def _write_v1_workflow(
    operator: opendal.AsyncOperator, record: temporaless_pb2.WorkflowRecord
) -> None:
    path = (
        "temporaless/v1/"
        f"namespace={record.key.namespace}/"
        f"workflow_id={record.key.workflow_id}/"
        f"run_id={record.key.run_id}/"
        "kind=workflow/record.binpb"
    )
    await operator.create_dir(path.rsplit("/", 1)[0] + "/")
    await operator.write(path, record.SerializeToString(deterministic=True))


async def _write_v1_blob(
    operator: opendal.AsyncOperator,
    namespace: str,
    workflow_id: str,
    run_id: str,
    data: bytes,
) -> None:
    path = (
        "temporaless/v1/"
        f"namespace={namespace}/"
        f"workflow_id={workflow_id}/"
        f"run_id={run_id}/"
        "kind=workflow/record.binpb"
    )
    await operator.create_dir(path.rsplit("/", 1)[0] + "/")
    await operator.write(path, data)


def _workflow_record(
    workflow_id: str, run_id: str, *, completed_at: datetime
) -> temporaless_pb2.WorkflowRecord:
    created = Timestamp()
    created.FromDatetime(datetime(2026, 7, 1, tzinfo=UTC))
    completed = Timestamp()
    completed.FromDatetime(completed_at)
    return temporaless_pb2.WorkflowRecord(
        schema_version=WORKFLOW_RECORD_SCHEMA_VERSION,
        key=temporaless_pb2.WorkflowKey(
            namespace="default",
            workflow_id=workflow_id,
            run_id=run_id,
        ),
        workflow_type="workflow:google.protobuf.StringValue->google.protobuf.StringValue",
        status=temporaless_pb2.WORKFLOW_STATUS_COMPLETED,
        created_at=created,
        completed_at=completed,
    )
