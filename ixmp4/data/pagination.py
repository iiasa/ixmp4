from typing import Any, Generic, List, TypeVar

import pydantic as pyd

from ixmp4.conf.settings import Settings

from .dataframe import DataFrameTypeAdapter

ResultsT = TypeVar("ResultsT")

default_settings = Settings()


class Pagination(pyd.BaseModel):
    limit: int = pyd.Field(
        default=default_settings.server.default_page_size,
        ge=0,
        le=default_settings.server.max_page_size,
    )
    offset: int = pyd.Field(default=0, ge=0)


class PaginationResult(pyd.BaseModel):
    limit: int = pyd.Field(ge=0)
    offset: int = pyd.Field(default=0, ge=0)
    model_config = pyd.ConfigDict(from_attributes=True)


class PaginatedResult(pyd.BaseModel, Generic[ResultsT]):
    results: ResultsT
    total: int
    pagination: PaginationResult | Pagination
    model_config = pyd.ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


GenericPaginatedResult = PaginatedResult[List[Any] | DataFrameTypeAdapter]
