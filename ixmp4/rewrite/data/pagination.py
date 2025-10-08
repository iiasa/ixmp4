from typing import Generic, TypeVar

import pydantic as pyd

from ixmp4.rewrite.conf import settings

ResultsT = TypeVar("ResultsT")


class Pagination(pyd.BaseModel):
    limit: int = pyd.Field(
        default=settings.default_page_size,
        ge=0,
        le=settings.max_page_size,
    )
    offset: int = pyd.Field(default=0, ge=0)


class PaginatedResult(pyd.BaseModel, Generic[ResultsT]):
    results: ResultsT
    total: int
    pagination: Pagination
    model_config = pyd.ConfigDict(from_attributes=True, arbitrary_types_allowed=True)
