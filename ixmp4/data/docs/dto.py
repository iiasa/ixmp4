from ixmp4.data.base.dto import BaseModel


class Docs(BaseModel):
    """Docs data model for various data types."""

    description: str
    "Text content of the docs object."
    dimension__id: int
    "Id of the related row."

    def __str__(self) -> str:
        return f"<Docs {self.id} dimension__id={self.dimension__id}>"
