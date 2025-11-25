from ixmp4.rewrite.data.base.dto import BaseModel, HasCreationInfo


class Variable(BaseModel, HasCreationInfo):
    """Optimization variable data model."""

    id: int
    name: str
    "Name of the variable."
    data: dict[str, list[float] | list[int] | list[str]]
    indexset_names: list[str] | None
    column_names: list[str] | None
    run__id: int

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
