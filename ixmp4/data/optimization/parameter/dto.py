from typing import Any

from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Parameter(BaseModel, HasCreationInfo):
    """Optimization parameter data model."""

    id: int
    name: str
    "Name of the parameter."

    data: dict[str, Any]
    indexset_names: list[str]
    column_names: list[str] | None
    run__id: int

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
