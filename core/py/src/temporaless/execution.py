from __future__ import annotations

from typing import Protocol

from temporaless.v1 import temporaless_pb2 as pb


class FencedExecutionUnsupportedError(RuntimeError):
    """Requested fencing has no qualified implementation; never fall back."""


def current_fenced_execution_capability(value: int) -> pb.FencedExecutionCapability:
    if value in (
        pb.FENCED_EXECUTION_CAPABILITY_UNSPECIFIED,
        pb.FENCED_EXECUTION_CAPABILITY_UNSUPPORTED,
    ):
        return pb.FENCED_EXECUTION_CAPABILITY_UNSUPPORTED
    raise FencedExecutionUnsupportedError(
        f"fenced execution is not implemented: capability {value}"
    )


class FencedExecutionStore(Protocol):
    """Reserved atomic boundary; no bundled store implements this protocol.

    Implementation requires every mutation, timer repair and retention path to
    obey the protobuf authority contract. A conditional claim is insufficient.
    """

    async def get_store_capabilities(
        self, request: pb.GetStoreCapabilitiesRequest
    ) -> pb.GetStoreCapabilitiesResponse: ...

    async def acquire_execution(
        self, request: pb.AcquireExecutionRequest
    ) -> pb.AcquireExecutionResponse: ...

    async def renew_execution(
        self, request: pb.RenewExecutionRequest
    ) -> pb.RenewExecutionResponse: ...

    async def release_execution(
        self, request: pb.ReleaseExecutionRequest
    ) -> pb.ReleaseExecutionResponse: ...

    async def apply_execution_mutations(
        self, request: pb.ApplyExecutionMutationsRequest
    ) -> pb.ApplyExecutionMutationsResponse: ...

    async def get_execution_operation(
        self, request: pb.GetExecutionOperationRequest
    ) -> pb.GetExecutionOperationResponse: ...
