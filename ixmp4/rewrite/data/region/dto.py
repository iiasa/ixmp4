from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Region(BaseModel):
    """Region data model."""

    name: str
    "Unique name of the region."
    hierarchy: str
    "Region hierarchy."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"
