import asyncio
import tempfile

import opendal
from google.protobuf.wrappers_pb2 import StringValue

from temporaless.storage import OpenDALStore
from temporaless.workflow import (
    ActivityOptions,
    ActivityWrapOptions,
    Options,
    Workflow,
    WorkflowWrapOptions,
    run,
    wrap_activity,
    wrap_workflow,
)


async def fetch_price_rpc(request: StringValue) -> StringValue:
    return StringValue(value=f"{request.value} 100.00")


async def fetch_price_workflow(workflow: Workflow, request: StringValue) -> StringValue:
    fetch = wrap_activity(
        ActivityWrapOptions[StringValue](
            workflow=workflow,
            options=ActivityOptions(activity_id="fetch:aapl"),
        ),
        StringValue,
    )(fetch_price_rpc)
    return await fetch(request)


async def main() -> None:
    operator = opendal.AsyncOperator("fs", root=tempfile.mkdtemp(prefix="temporaless-"))
    store = OpenDALStore(operator)

    handler = wrap_workflow(
        WorkflowWrapOptions[StringValue](
            store=store,
            options=Options(
                workflow_id="prices:aapl",
                run_id="2026-05-02",
            ),
        ),
        StringValue,
    )(fetch_price_rpc)
    price = await handler(StringValue(value="AAPL"))
    print(price.value)

    activity_price = await run(
        store,
        Options(workflow_id="prices:activity", run_id="2026-05-02"),
        StringValue(value="AAPL"),
        StringValue,
        fetch_price_workflow,
    )
    print(activity_price.value)


if __name__ == "__main__":
    asyncio.run(main())
