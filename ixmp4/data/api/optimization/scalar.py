from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class Scalar(base.BaseModel):
    NotFound: ClassVar = abstract.Scalar.NotFound
    NotUnique: ClassVar = abstract.Scalar.NotUnique
    DeletionPrevented: ClassVar = abstract.Scalar.DeletionPrevented

    id: int
    name: str
    value: float
    unit__id: int
    run__id: int

    created_at: datetime | None
    created_by: str | None


class ScalarDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/scalars/"


class ScalarRepository(
    base.Creator[Scalar],
    base.Retriever[Scalar],
    base.Enumerator[Scalar],
    abstract.ScalarRepository,
):
    model_class = Scalar
    prefix = "optimization/scalars/"
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = ScalarDocsRepository(self.client)

    def create(
        self,
        name: str,
        value: float,
        unit_id: int,
        run_id: int,
    ) -> Scalar:
        return super().create(name=name, value=value, unit_id=unit_id, run_id=run_id)

    def get(self, run_id: int, name: str, unit_id: int | None = None) -> Scalar | Iterable[Scalar]:
        if unit_id:
            return super().get(run_id=run_id, name=name, unit_id=unit_id)
        else:
            return super().list(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Scalar:
        res = self._get_by_id(id)
        return Scalar(**res)

    def list(self, *args, **kwargs) -> Iterable[Scalar]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Scalar] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
