import logging
from enum import Enum
from typing import Any, Literal

from ixmp4.data.meta.exceptions import InvalidRunMeta

logger = logging.getLogger(__name__)

PdDtype = Literal["Int64", "str", "float64", "boolean"]


class Type(str, Enum):
    INT = "INT"
    STR = "STR"
    FLOAT = "FLOAT"
    BOOL = "BOOL"

    @classmethod
    def from_pytype(cls, type_: type) -> "Type":
        return _type_map.get(type_, Type.STR)

    @classmethod
    def column_for_type(cls, type_: "Type") -> str:
        return _column_map.get(type_, "value_str")

    @classmethod
    def pd_dtype_for_type(cls, type_: "Type") -> PdDtype:
        return _pd_dtype_map[type_]

    @classmethod
    def columns(cls) -> list[str]:
        return list(_column_map.values())

    def __str__(self) -> str:
        return self.value


def convert_value(value: Any) -> tuple[Type, Any]:
    value_type = type(value)

    if type(value) in _type_map:
        return value

    logger.warning("Converting value of type '%s' to string.", value_type.__name__)
    try:
        return str(value)
    except Exception as e:
        raise InvalidRunMeta(
            f"Failed to convert value of type '{value_type.__name__}' to string: {e}"
        ) from e


_type_map: dict[type, Type] = {
    int: Type.INT,
    str: Type.STR,
    float: Type.FLOAT,
    bool: Type.BOOL,
}

_column_map: dict[str, str] = {
    Type.INT: "value_int",
    Type.STR: "value_str",
    Type.FLOAT: "value_float",
    Type.BOOL: "value_bool",
}

_pd_dtype_map: dict[str, PdDtype] = {
    Type.INT: "Int64",
    Type.STR: "str",
    Type.FLOAT: "float64",
    Type.BOOL: "boolean",
}
