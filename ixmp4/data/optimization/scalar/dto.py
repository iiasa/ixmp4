from ixmp4.data.base.dto import BaseModel, HasCreationInfo
from ixmp4.data.unit.dto import Unit


class Scalar(BaseModel, HasCreationInfo):
    """Optimization scalar data model."""

    id: int
    name: str
    "Name of the scalar."

    value: float
    unit: Unit
    unit__id: int
    run__id: int

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
