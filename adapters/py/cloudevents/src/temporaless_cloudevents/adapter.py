"""Publish best-effort CloudEvents after successful point-storage RPCs."""

from __future__ import annotations

import inspect
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

from cloudevents.core.v1.event import CloudEvent
from connectrpc.request import RequestContext
from google.protobuf.message import Message
from temporaless.v1 import temporaless_pb2

_LOGGER = logging.getLogger(__name__)
_SERVICE = temporaless_pb2.DESCRIPTOR.services_by_name["RecordStoreService"]
_RECORD_METHODS = frozenset(
    ("PutWorkflow", "PutActivity", "PutTimer", "PutEvent", "DeliverEvent", "TryCreateClaim")
)
_KEY_METHODS = frozenset(
    ("DeleteWorkflow", "DeleteActivity", "DeleteTimer", "DeleteEvent", "DeleteClaim", "DeleteRun")
)


@dataclass(frozen=True)
class Options:
    """Application-owned event identity and async publication boundary.

    ``new_id`` must return a new nonempty string per observation, unique within
    ``source``. A publisher retry must retain the original event and ID.
    """

    source: str
    new_id: Callable[[], str]
    publish: Callable[[CloudEvent], Awaitable[None]]


class RecordStoreInterceptor:
    """Server interceptor for ``RecordStoreService`` mutation observations.

    Attach only to the storage ASGI application. These are invalidations:
    successful duplicate deletes, deliveries and unsuccessful claim attempts
    also publish. Consumers read current state from authoritative storage.
    """

    def __init__(self, options: Options) -> None:
        if not isinstance(options.source, str) or not options.source.strip():
            raise ValueError("CloudEvents source is required")
        if (
            any(
                character.isspace() or ord(character) < 32 or 127 <= ord(character) <= 159
                for character in options.source
            )
            or not urlsplit(options.source).scheme
            or re.search(r"%(?![0-9a-fA-F]{2})", options.source)
        ):
            raise ValueError(
                "CloudEvents source must be an absolute URI without whitespace or controls"
            )
        if not callable(options.new_id) or (
            inspect.iscoroutinefunction(options.new_id)
            or inspect.iscoroutinefunction(options.new_id.__call__)
        ):
            raise TypeError("CloudEvents new_id must be a synchronous callable")
        publisher = options.publish
        if not callable(publisher) or not (
            inspect.iscoroutinefunction(publisher)
            or inspect.iscoroutinefunction(publisher.__call__)
        ):
            raise TypeError("CloudEvents publish must be an async callable")
        self._options = options

    async def intercept_unary[RequestT, ResponseT](
        self,
        call_next: Callable[[RequestT, RequestContext], Awaitable[ResponseT]],
        request: RequestT,
        ctx: RequestContext,
    ) -> ResponseT:
        name = ctx.method.name
        if ctx.method.service_name != _SERVICE.full_name or name not in (
            _RECORD_METHODS | _KEY_METHODS
        ):
            return await call_next(request, ctx)
        method = _SERVICE.methods_by_name[name]
        if not isinstance(request, Message) or method.input_type != request.DESCRIPTOR:
            return await call_next(request, ctx)

        # Snapshot only the identity before downstream middleware can mutate
        # the request. Never serialize record payloads, failures or annotations.
        data: bytes | None = None
        schema = ""
        try:
            container = getattr(request, "record") if name in _RECORD_METHODS else request  # noqa: B009
            supplied_key = getattr(container, "key")  # noqa: B009
            key = type(supplied_key)()
            key.CopyFrom(supplied_key)
            key.DiscardUnknownFields()
            data = key.SerializeToString(deterministic=True)
            schema = f"https://type.googleapis.com/{key.DESCRIPTOR.full_name}"
        except Exception:
            _LOGGER.exception("CloudEvents identity snapshot failed for %s", method.full_name)

        response = await call_next(request, ctx)
        if data is not None:
            try:
                event_id = self._options.new_id()
                if (
                    not isinstance(event_id, str)
                    or not event_id
                    or event_id != event_id.strip()
                    or any(
                        ord(character) < 32 or 127 <= ord(character) <= 159
                        for character in event_id
                    )
                ):
                    raise ValueError(
                        "CloudEvents new_id must return a nonempty string without controls "
                        "or surrounding whitespace"
                    )
                event = CloudEvent(
                    {
                        "specversion": "1.0",
                        "id": event_id,
                        "source": self._options.source,
                        "type": method.full_name,
                        "datacontenttype": "application/protobuf",
                        "dataschema": schema,
                    },
                    data,
                )
                await self._options.publish(event)
            except Exception:
                _LOGGER.exception(
                    "CloudEvents publication failed for %s; storage RPC succeeded",
                    method.full_name,
                )
        return response
