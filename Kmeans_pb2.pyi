from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Centroids(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: float
    def __init__(self, data: _Optional[float] = ...) -> None: ...

class Data(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: float
    value: float
    def __init__(self, key: _Optional[float] = ..., value: _Optional[float] = ...) -> None: ...

class MasterToMapperReq(_message.Message):
    __slots__ = ("path", "start_index", "end_index", "prev_Centroids")
    PATH_FIELD_NUMBER: _ClassVar[int]
    START_INDEX_FIELD_NUMBER: _ClassVar[int]
    END_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    path: str
    start_index: int
    end_index: int
    prev_Centroids: _containers.RepeatedCompositeFieldContainer[Centroids]
    def __init__(self, path: _Optional[str] = ..., start_index: _Optional[int] = ..., end_index: _Optional[int] = ..., prev_Centroids: _Optional[_Iterable[_Union[Centroids, _Mapping]]] = ...) -> None: ...

class MasterToMapperRes(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: int
    def __init__(self, success: _Optional[int] = ...) -> None: ...

class MasterToReducerReq(_message.Message):
    __slots__ = ("start_process",)
    START_PROCESS_FIELD_NUMBER: _ClassVar[int]
    start_process: int
    def __init__(self, start_process: _Optional[int] = ...) -> None: ...

class MasterToReducerRes(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: int
    def __init__(self, success: _Optional[int] = ...) -> None: ...

class ReducerToMapperReq(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class ReducerToMapperRes(_message.Message):
    __slots__ = ("data", "success")
    DATA_FIELD_NUMBER: _ClassVar[int]
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    data: _containers.RepeatedCompositeFieldContainer[Data]
    success: int
    def __init__(self, data: _Optional[_Iterable[_Union[Data, _Mapping]]] = ..., success: _Optional[int] = ...) -> None: ...
