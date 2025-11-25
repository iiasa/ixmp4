import enum
from typing import TypedDict


class TypeColumnsDict(TypedDict):
    step_year: bool
    step_category: bool
    step_datetime: bool


class Type(str, enum.Enum):
    ANNUAL = "ANNUAL"
    CATEGORICAL = "CATEGORICAL"
    DATETIME = "DATETIME"

    @classmethod
    def columns_for_type(cls, type_: "Type") -> TypeColumnsDict:
        return _columns_for_types[type_]

    def __str__(self):
        return self.value


_columns_for_types: dict[Type, TypeColumnsDict] = {
    Type.ANNUAL: {
        "step_year": True,
        "step_category": False,
        "step_datetime": False,
    },
    Type.CATEGORICAL: {
        "step_year": True,
        "step_category": True,
        "step_datetime": False,
    },
    Type.DATETIME: {
        "step_year": False,
        "step_category": False,
        "step_datetime": True,
    },
}
