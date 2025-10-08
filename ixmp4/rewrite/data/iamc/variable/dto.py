from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Variable(BaseModel):
    """IAMC variable data model."""

    name: str
    "Unique name of the variable."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"
