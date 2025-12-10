from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Scenario(BaseModel, HasCreationInfo):
    """Modeling scenario data model."""

    name: str
    "Unique name of the scenario."

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"
