from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Region(BaseModel, HasCreationInfo):
    """Region data model."""

    name: str
    "Unique name of the region."
    hierarchy: str
    "Region hierarchy."

    def __str__(self) -> str:
        return f"<Region name='{self.name}' id={self.id}>"
