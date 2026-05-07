from enum import Enum


class Type(str, Enum):
    INT = "INT"
    STR = "STR"
    FLOAT = "FLOAT"

    @classmethod
    def from_pytype(cls, type_: type) -> "Type | None":
        return _type_map.get(type_, None)

    def to_pytype(self) -> type:
        return _reverse_type_map[self]

    def __str__(self) -> str | None:
        return self.value


_type_map: dict[type, Type] = {
    int: Type.INT,
    str: Type.STR,
    float: Type.FLOAT,
}

_reverse_type_map: dict[Type, type] = {et: pt for pt, et in _type_map.items()}
