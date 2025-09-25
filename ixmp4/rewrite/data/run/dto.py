from datetime import datetime
from typing import TYPE_CHECKING

from ixmp4.rewrite.data.base.dto import BaseModel

if TYPE_CHECKING:
    from ixmp4.rewrite.data.model.dto import Model
    from ixmp4.rewrite.data.scenario.dto import Scenario


class Run(BaseModel):
    """Model run data model."""

    model__id: int
    "Foreign unique integer id of the model."
    model: "Model"
    "Associated model."

    scenario__id: int
    "Foreign unique integer id of the scenario."
    scenario: "Scenario"
    "Associated scenario."

    version: int
    "Run version."
    is_default: bool
    "`True` if this is the default run version."

    created_at: datetime
    created_by: str

    updated_at: datetime
    updated_by: str

    lock_transaction: int | None

    def __str__(self) -> str:
        return f"<Run {self.id} model={self.model.name} \
            scenario={self.scenario.name} version={self.version} \
            is_default={self.is_default}>"
