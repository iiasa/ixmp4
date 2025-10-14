from ixmp4.rewrite.data.base.dto import BaseModel, HasCreationInfo


class Variable(BaseModel, HasCreationInfo):
    """IAMC variable data model."""

    name: str
    "Unique name of the variable."

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
