"""Protobuf-7 Temporaless workflow server for the Dagster boundary proof."""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys
from pathlib import Path

import google.protobuf
import opendal
import uvicorn
from google.protobuf.descriptor_pb2 import FileDescriptorSet
from temporaless.storage import OpenDALStore
from temporaless.workflow import Options
from temporaless_connectworkflow import WorkflowMethodWrapOptions, wrap_workflow_method

_SERVER_GEN = Path(__file__).resolve().parent / "server_gen"
sys.path.insert(0, str(_SERVER_GEN))

from compat.v1 import workflow_connect, workflow_pb2  # noqa: E402


def _source_descriptor(descriptor_set_path: Path) -> bytes:
    descriptor_set = FileDescriptorSet.FromString(descriptor_set_path.read_bytes())
    for descriptor in descriptor_set.file:
        if descriptor.name == "compat/v1/workflow.proto":
            return descriptor.SerializeToString(deterministic=True)
    raise RuntimeError("compiled descriptor set does not contain compat/v1/workflow.proto")


def _record_body_call(path: Path, workflow_id: str, run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(f"{workflow_id}\t{run_id}\n")
        stream.flush()
        os.fsync(stream.fileno())


class _WorkflowService(workflow_connect.WorkflowService):
    def __init__(self, store: OpenDALStore, side_effect_path: Path) -> None:
        self._store = store
        self._side_effect_path = side_effect_path

    @wrap_workflow_method(
        WorkflowMethodWrapOptions(
            store=lambda self: self._store,  # type: ignore[attr-defined]  # ty: ignore[unresolved-attribute]
            result_type=workflow_pb2.RunResponse,
            options_for=lambda _self, request: Options(
                workflow_id=request.workflow_id,
                run_id=request.run_id,
            ),
        )
    )
    async def run(
        self,
        request: workflow_pb2.RunRequest,
        _ctx: object = None,
    ) -> workflow_pb2.RunResponse:
        await asyncio.to_thread(
            _record_body_call,
            self._side_effect_path,
            request.workflow_id,
            request.run_id,
        )
        return workflow_pb2.RunResponse(value=f"workflow:{request.value}")


async def _serve(args: argparse.Namespace) -> None:
    protobuf_major = int(google.protobuf.__version__.split(".", 1)[0])
    if protobuf_major != 7:
        raise RuntimeError(f"compatibility proof requires protobuf 7, found {protobuf_major}")

    source_descriptor = _source_descriptor(args.descriptor_set)
    generated_descriptor = workflow_pb2.DESCRIPTOR.serialized_pb
    if source_descriptor != generated_descriptor:
        raise RuntimeError(
            "protobuf-7 server fixture is stale; regenerate with "
            "`buf generate --template adapters/py/dagstercompat/buf.gen.yaml`"
        )

    args.store_root.mkdir(parents=True, exist_ok=True)
    store = OpenDALStore(opendal.AsyncOperator("fs", root=str(args.store_root)))
    app = workflow_connect.WorkflowServiceASGIApplication(
        _WorkflowService(store, args.side_effect_file)
    )

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(2048)
    endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}"
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="warning",
            loop="asyncio",
            lifespan="off",
        )
    )
    serve_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        while not server.started:
            if serve_task.done():
                await serve_task
            await asyncio.sleep(0.01)

        ready_tmp = args.ready_file.with_name(f".{args.ready_file.name}.{os.getpid()}.tmp")
        ready_tmp.write_text(endpoint, encoding="utf-8")
        os.replace(ready_tmp, args.ready_file)

        while not args.stop_file.exists():
            if serve_task.done():
                await serve_task
            await asyncio.sleep(0.02)
        server.should_exit = True
        await serve_task
    finally:
        listener.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-root", required=True, type=Path)
    parser.add_argument("--side-effect-file", required=True, type=Path)
    parser.add_argument("--ready-file", required=True, type=Path)
    parser.add_argument("--stop-file", required=True, type=Path)
    parser.add_argument("--descriptor-set", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(_serve(_parse_args()))
