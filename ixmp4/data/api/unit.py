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


class Unit(base.BaseModel):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    id: int
    name: str
    created_at: datetime | None = Field(None)
    created_by: str | None = Field(None)


class UnitDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/units/"


class UnitRepository(
    base.Creator[Unit],
    base.Deleter[Unit],
    base.Retriever[Unit],
    base.Enumerator[Unit],
    base.VersionManager[Unit],
    abstract.UnitRepository,
):
    model_class = Unit
    prefix = "units/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = UnitDocsRepository(self.backend)

    def create(self, name: str) -> Unit:
        return super().create(name=name)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get(self, name: str) -> Unit:
        return super().get(name=name)

    def get_by_id(self, id: int) -> Unit:
        res = self._get_by_id(id)
        return Unit(**res)

    def enumerate(
        self, **kwargs: Unpack[abstract.unit.EnumerateKwargs]
    ) -> list[Unit] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs: Unpack[abstract.unit.EnumerateKwargs]) -> list[Unit]:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(self, **kwargs: Unpack[abstract.unit.EnumerateKwargs]) -> pd.DataFrame:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._tabulate(json=json)
