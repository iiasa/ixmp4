from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Unit(BaseModel):
    """Unit data model."""

    name: str
    "Unique name of the unit."
    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"
