from typing import Generic, TypeVar

import pandas as pd
from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict, Field

from ixmp4.conf import settings
from ixmp4.data import api


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


EnumeratedT = TypeVar("EnumeratedT")


class Pagination(BaseModel):
    limit: int = Field(
        default=settings.default_page_size,
        ge=0,
        le=settings.max_page_size,
    )
    offset: int = Field(default=0, ge=0)


class EnumerationOutput(BaseModel, Generic[EnumeratedT]):
    pagination: Pagination
    total: int
    results: api.DataFrame | list[EnumeratedT]

    def __init__(
        __pydantic_self__,
        *args,
        results: pd.DataFrame | api.DataFrame | list[EnumeratedT],
        **kwargs,
    ):
        kwargs["results"] = (
            api.DataFrame.model_validate(results)
            if isinstance(results, pd.DataFrame)
            else results
        )
        super().__init__(**kwargs)
