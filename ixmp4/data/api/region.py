from datetime import datetime
from typing import ClassVar

import pandas as pd
from pydantic import Field

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class RegionParent(base.BaseModel):
    id: int
    name: str
    hierarchy: str


class Region(base.BaseModel):
    NotFound: ClassVar = abstract.Region.NotFound
    NotUnique: ClassVar = abstract.Region.NotUnique
    DeletionPrevented: ClassVar = abstract.Region.DeletionPrevented

    id: int
    name: str
    hierarchy: str

    created_at: datetime | None = Field(None)
    created_by: str | None = Field(None)


class RegionDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/regions/"


class RegionRepository(
    base.Creator[Region],
    base.Deleter[Region],
    base.Retriever[Region],
    base.Enumerator[Region],
    abstract.RegionRepository,
):
    model_class = Region
    prefix = "regions/"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = RegionDocsRepository(self.backend)

    def create(
        self,
        name: str,
        hierarchy: str,
    ) -> Region:
        return super().create(name=name, hierarchy=hierarchy)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get(self, name: str) -> Region:
        return super().get(name=name)

    def enumerate(self, **kwargs) -> list[Region] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[Region]:
        return super()._list(json=kwargs)

    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs)
