"""Importable Prefect deployment fixture used to prove worker reload."""

from google.protobuf.wrappers_pb2 import StringValue

from temporaless_prefectcompat import WorkflowWrapOptions, wrap_workflow


async def deployed_echo(req: StringValue) -> StringValue:
    return StringValue(value=f"deployed:{req.value}")


DeployedEchoFlow = wrap_workflow(
    deployed_echo,
    WorkflowWrapOptions(name="DeployedEchoFlow"),
)
