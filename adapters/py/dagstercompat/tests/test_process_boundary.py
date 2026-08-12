"""Real Dagster -> protobuf-7 Temporaless ConnectRPC boundary proof."""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import dagster
import google.protobuf
import pytest
from compat.v1 import workflow_connect, workflow_pb2
from google.protobuf.descriptor_pb2 import FileDescriptorSet

_REPOSITORY_ROOT = Path(__file__).resolve().parents[4]
_PROTO_ROOT = Path("adapters/py/dagstercompat/tests/proto")
_SERVER_SCRIPT = Path(__file__).resolve().parent / "temporaless_server.py"


def _compile_source_descriptor(destination: Path) -> bytes:
    buf = shutil.which("buf")
    if buf is None:
        pytest.fail("Buf is required; run this compatibility proof through `flox activate`")
    completed = subprocess.run(
        [
            buf,
            "build",
            str(_PROTO_ROOT),
            "--as-file-descriptor-set",
            "--exclude-source-info",
            "-o",
            str(destination),
        ],
        cwd=_REPOSITORY_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if completed.returncode != 0:
        pytest.fail(f"Buf could not compile the Dagster fixture:\n{completed.stderr}")

    descriptor_set = FileDescriptorSet.FromString(destination.read_bytes())
    for descriptor in descriptor_set.file:
        if descriptor.name == "compat/v1/workflow.proto":
            return descriptor.SerializeToString(deterministic=True)
    pytest.fail("compiled descriptor set does not contain compat/v1/workflow.proto")


def _terminate(process: subprocess.Popen[str]) -> tuple[str, str]:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
    try:
        return process.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        return process.communicate(timeout=5)


@contextmanager
def _running_temporaless_server(
    tmp_path: Path,
    descriptor_set_path: Path,
) -> Iterator[tuple[str, Path, Path]]:
    uv = shutil.which("uv")
    if uv is None:
        pytest.fail("uv is required to launch the isolated Temporaless environment")

    ready_file = tmp_path / "server.ready"
    stop_file = tmp_path / "server.stop"
    store_root = tmp_path / "store"
    side_effect_file = tmp_path / "body-calls.tsv"
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment.pop("PYTHONHOME", None)
    environment.pop("VIRTUAL_ENV", None)
    environment["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        [
            uv,
            "run",
            "--frozen",
            "--project",
            str(_REPOSITORY_ROOT / "core/py"),
            "python",
            str(_SERVER_SCRIPT),
            "--store-root",
            str(store_root),
            "--side-effect-file",
            str(side_effect_file),
            "--ready-file",
            str(ready_file),
            "--stop-file",
            str(stop_file),
            "--descriptor-set",
            str(descriptor_set_path),
        ],
        cwd=_REPOSITORY_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )

    deadline = time.monotonic() + 30
    while not ready_file.exists():
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            pytest.fail(
                "isolated Temporaless server exited before readiness\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        if time.monotonic() >= deadline:
            stdout, stderr = _terminate(process)
            pytest.fail(
                "isolated Temporaless server did not become ready\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        time.sleep(0.02)

    endpoint = ready_file.read_text(encoding="utf-8")
    try:
        yield endpoint, store_root, side_effect_file
    finally:
        stop_file.touch()
        try:
            stdout, stderr = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            stdout, stderr = _terminate(process)
        assert process.returncode == 0, (
            f"isolated Temporaless server shutdown failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        )


def test_dagster_generated_code_matches_application_proto(tmp_path: Path) -> None:
    source_descriptor = _compile_source_descriptor(tmp_path / "workflow-descriptor.binpb")
    assert workflow_pb2.DESCRIPTOR.serialized_pb == source_descriptor, (
        "protobuf-6 Dagster fixture is stale; regenerate with "
        "`buf generate --template adapters/py/dagstercompat/buf.gen.yaml`"
    )


def test_real_dagster_job_replays_on_temporaless_server(tmp_path: Path) -> None:
    # Dagster and Temporaless intentionally stay in incompatible protobuf
    # environments. The server performs the reciprocal protobuf-7 guard.
    assert int(google.protobuf.__version__.split(".", 1)[0]) == 6
    with pytest.raises(ModuleNotFoundError):
        __import__("temporaless")

    descriptor_set_path = tmp_path / "workflow-descriptor.binpb"
    source_descriptor = _compile_source_descriptor(descriptor_set_path)
    assert workflow_pb2.DESCRIPTOR.serialized_pb == source_descriptor

    with _running_temporaless_server(
        tmp_path,
        descriptor_set_path,
    ) as (endpoint, store_root, side_effect_file):

        async def invoke() -> workflow_pb2.RunResponse:
            async with workflow_connect.WorkflowServiceClient(
                endpoint,
                timeout_ms=5_000,
            ) as client:
                return await client.run(
                    workflow_pb2.RunRequest(
                        workflow_id="prices:AAPL",
                        run_id="dagster:2026-08-12",
                        value="AAPL",
                    )
                )

        @dagster.op
        def call_workflow() -> tuple[str, bool]:
            response = asyncio.run(invoke())
            return response.value, response.replayed

        @dagster.job
        def workflow_job() -> None:
            call_workflow()

        first = workflow_job.execute_in_process()
        second = workflow_job.execute_in_process()

        assert first.success and second.success
        assert first.output_for_node("call_workflow") == ("workflow:AAPL", False)
        assert second.output_for_node("call_workflow") == ("workflow:AAPL", False)

        # The second response is the first workflow result replayed from the
        # protobuf record. The application body and its side effect ran once.
        assert side_effect_file.read_text(encoding="utf-8").splitlines() == [
            "prices:AAPL\tdagster:2026-08-12"
        ]
        workflow_record = (
            store_root / "temporaless/v2/default/prices:AAPL/dagster:2026-08-12/workflow.binpb"
        )
        assert workflow_record.is_file()
        assert workflow_record.stat().st_size > 0
