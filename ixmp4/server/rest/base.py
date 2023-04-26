import pydantic


class BaseModel(pydantic.BaseModel):
    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
