from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class RunRequest(_message.Message):
    __slots__ = ("workflow_id", "run_id", "value")
    WORKFLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    workflow_id: str
    run_id: str
    value: str
    def __init__(self, workflow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class RunResponse(_message.Message):
    __slots__ = ("value", "replayed")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    value: str
    replayed: bool
    def __init__(self, value: _Optional[str] = ..., replayed: _Optional[bool] = ...) -> None: ...
