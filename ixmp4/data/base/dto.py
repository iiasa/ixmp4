from datetime import datetime

import pydantic as pyd


class BaseModel(pyd.BaseModel):
    id: int
    "Integer id."

    model_config = pyd.ConfigDict(from_attributes=True)


class HasCreationInfo(pyd.BaseModel):
    created_at: datetime | None
    "Creation date/time."
    created_by: str | None
    "Creator."


class HasUpdateInfo(pyd.BaseModel):
    updated_at: datetime | None
    "Creation date/time."
    updated_by: str | None
    "Creator."
