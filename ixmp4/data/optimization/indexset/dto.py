from ixmp4.data.base.dto import BaseModel, HasCreationInfo

from .type import Type


class IndexSet(BaseModel, HasCreationInfo):
    """Optimization index set model."""

    id: int
    name: str
    "Name of the index set."
    run__id: int
    "Id of the linked run."

    data: list[int] | list[float] | list[str]
    data_type: Type | None

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
