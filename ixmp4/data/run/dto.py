from ixmp4.data.base.dto import BaseModel, HasCreationInfo, HasUpdateInfo
from ixmp4.data.model.dto import Model
from ixmp4.data.scenario.dto import Scenario


class Run(BaseModel, HasCreationInfo, HasUpdateInfo):
    """Model run data model."""

    model__id: int
    "Foreign unique integer id of the model."
    model: Model
    "Associated model."

    scenario__id: int
    "Foreign unique integer id of the scenario."
    scenario: Scenario
    "Associated scenario."

    version: int
    "Run version."
    is_default: bool
    "`True` if this is the default run version."

    lock_transaction: int | None

    def __str__(self) -> str:
        return f"<Run {self.id} model={self.model.name} \
            scenario={self.scenario.name} version={self.version} \
            is_default={self.is_default}>"
