from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Centroids(_message.Message):
    __slots__ = ("x_cord", "y_cord")
    X_CORD_FIELD_NUMBER: _ClassVar[int]
    Y_CORD_FIELD_NUMBER: _ClassVar[int]
    x_cord: float
    y_cord: float
    def __init__(self, x_cord: _Optional[float] = ..., y_cord: _Optional[float] = ...) -> None: ...

class Point(_message.Message):
    __slots__ = ("x", "y")
    X_FIELD_NUMBER: _ClassVar[int]
    Y_FIELD_NUMBER: _ClassVar[int]
    x: float
    y: float
    def __init__(self, x: _Optional[float] = ..., y: _Optional[float] = ...) -> None: ...

class Data(_message.Message):
    __slots__ = ("key", "point")
    KEY_FIELD_NUMBER: _ClassVar[int]
    POINT_FIELD_NUMBER: _ClassVar[int]
    key: float
    point: Point
    def __init__(self, key: _Optional[float] = ..., point: _Optional[_Union[Point, _Mapping]] = ...) -> None: ...

class MasterToMapperReq(_message.Message):
    __slots__ = ("start_index", "end_index", "mapper_index", "prev_Centroids", "reducer_count")
    START_INDEX_FIELD_NUMBER: _ClassVar[int]
    END_INDEX_FIELD_NUMBER: _ClassVar[int]
    MAPPER_INDEX_FIELD_NUMBER: _ClassVar[int]
    PREV_CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    REDUCER_COUNT_FIELD_NUMBER: _ClassVar[int]
    start_index: int
    end_index: int
    mapper_index: int
    prev_Centroids: _containers.RepeatedCompositeFieldContainer[Centroids]
    reducer_count: int
    def __init__(self, start_index: _Optional[int] = ..., end_index: _Optional[int] = ..., mapper_index: _Optional[int] = ..., prev_Centroids: _Optional[_Iterable[_Union[Centroids, _Mapping]]] = ..., reducer_count: _Optional[int] = ...) -> None: ...

class MasterToMapperRes(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: int
    def __init__(self, success: _Optional[int] = ...) -> None: ...

class MasterToReducerReq(_message.Message):
    __slots__ = ("start_process", "id", "M")
    START_PROCESS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    M_FIELD_NUMBER: _ClassVar[int]
    start_process: int
    id: int
    M: int
    def __init__(self, start_process: _Optional[int] = ..., id: _Optional[int] = ..., M: _Optional[int] = ...) -> None: ...

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

class MapperToReducerRes(_message.Message):
    __slots__ = ("success",)
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    success: int
    def __init__(self, success: _Optional[int] = ...) -> None: ...
