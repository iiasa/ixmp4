from typing import Generic, TypeVar

import pydantic as pyd

from ixmp4.conf.settings import Settings

ResultsT = TypeVar("ResultsT")

default_settings = Settings()


class Pagination(pyd.BaseModel):
    limit: int = pyd.Field(
        default=default_settings.server.default_page_size,
        ge=0,
        le=default_settings.server.max_page_size,
    )
    offset: int = pyd.Field(default=0, ge=0)


class PaginatedResult(pyd.BaseModel, Generic[ResultsT]):
    results: ResultsT
    total: int
    pagination: Pagination
    model_config = pyd.ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
