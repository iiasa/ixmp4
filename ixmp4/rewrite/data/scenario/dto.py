from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Scenario(BaseModel):
    """Modeling scenario data model."""

    name: str
    "Unique name of the scenario."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"
