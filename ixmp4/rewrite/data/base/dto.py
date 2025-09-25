import pydantic as pyd


class BaseModel(pyd.BaseModel):
    id: int
    "Integer id."

    model_config = pyd.ConfigDict(from_attributes=True)
