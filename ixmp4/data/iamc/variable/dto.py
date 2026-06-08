from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Variable(BaseModel, HasCreationInfo):
    """IAMC variable data model."""

    name: str
    "Unique name of the variable."

    def __str__(self) -> str:
        return f"<Variable name='{self.name}' id={self.id}>"
