from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Docs(BaseModel):
    """Docs data model for various data types."""

    description: str
    "Text content of the docs object."
    dimension__id: int
    "Id of the related row."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Docs {self.id} dimension__id={self.dimension__id}>"
