from ixmp4.data.base.dto import BaseModel, HasCreationInfo


class Model(BaseModel, HasCreationInfo):
    """Data model of an assement modeling "model".
    Unfortunately two naming conventions clash here.
    """

    name: str
    "Unique name of the model."

    def __str__(self) -> str:
        return f"<Model {self.id} name={self.name}>"
