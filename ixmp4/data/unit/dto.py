from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Unit(BaseModel, HasCreationInfo):
    """Unit data model."""

    name: str
    "Unique name of the unit."

    def __str__(self) -> str:
        return f"<Unit name='{self.name}' id={self.id}>"
