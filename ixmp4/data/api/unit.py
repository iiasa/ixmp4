from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class Unit(base.BaseModel):
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    id: int
    name: str
    created_at: datetime | None
    created_by: str | None


class UnitDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/units/"


class UnitRepository(
    base.Creator[Unit],
    base.Deleter[Unit],
    base.Retriever[Unit],
    base.Enumerator[Unit],
    abstract.UnitRepository,
):
    model_class = Unit
    prefix = "units/"
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = UnitDocsRepository(self.client)

    def create(
        self,
        name: str,
    ) -> Unit:
        return super().create(name=name)

    def delete(self, id: int) -> None:
        super().delete(id)

    def get(self, name: str) -> Unit:
        return super().get(name=name)

    def list(self, *args, **kwargs) -> Iterable[Unit]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Unit] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
