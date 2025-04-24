from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

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
    base.VersionManager[Region],
    abstract.RegionRepository,
):
    model_class = Region
    prefix = "regions/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = RegionDocsRepository(self.backend)

    def create(self, name: str, hierarchy: str) -> Region:
        return super().create(name=name, hierarchy=hierarchy)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get(self, name: str) -> Region:
        return super().get(name=name)

    def enumerate(
        self, **kwargs: Unpack[abstract.region.EnumerateKwargs]
    ) -> list[Region] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs: Unpack[abstract.region.EnumerateKwargs]) -> list[Region]:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.region.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._tabulate(json=json)
