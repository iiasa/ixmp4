from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

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

    created_at: datetime | None
    created_by: str | None


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
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = RegionDocsRepository(self.client)

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

    def list(self, *args, **kwargs) -> Iterable[Region]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Region] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
