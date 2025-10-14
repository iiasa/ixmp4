from ixmp4.rewrite.data.base.dto import BaseModel, HasCreationInfo


class Region(BaseModel, HasCreationInfo):
    """Region data model."""

    name: str
    "Unique name of the region."
    hierarchy: str
    "Region hierarchy."

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"
