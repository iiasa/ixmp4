from enum import Enum
from typing import Any, Literal
from ixmp4.data.meta.exceptions import InvalidRunMeta
from pandas.core.dtypes.inference import is_list_like

PdDtype = Literal["Int64", "str", "float64", "boolean"]


class Type(str, Enum):
    INT = "INT"
    STR = "STR"
    FLOAT = "FLOAT"
    BOOL = "BOOL"

    @classmethod
    def from_pytype(cls, type_: type) -> "Type":
        return _type_map[type_]

    @classmethod
    def column_for_type(cls, type_: "Type") -> str:
        return _column_map[type_]

    @classmethod
    def pd_dtype_for_type(cls, type_: "Type") -> PdDtype:
        return _pd_dtype_map[type_]

    @classmethod
    def columns(cls) -> list[str]:
        return list(_column_map.values())

    def __str__(self) -> str:
        return self.value


def check_meta_type(value: Any) -> Any:
    if is_list_like(value):
        for v in value:
            check_meta_type(v)

    if type(value) not in _type_map:
        raise InvalidRunMeta(
            f"Invalid meta indicator '{value}', allowed types: {list(_type_map.keys())}"
        )
    return value


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
