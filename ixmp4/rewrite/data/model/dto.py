from datetime import datetime

from ixmp4.rewrite.data.base.dto import BaseModel


class Model(BaseModel):
    """Data model of an assement modeling "model".
    Unfortunately two naming conventions clash here.
    """

    name: str
    "Unique name of the model."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"
