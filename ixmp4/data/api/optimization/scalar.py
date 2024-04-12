from datetime import datetime
from typing import Any, ClassVar, Iterable, Mapping

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository
from ..unit import Unit


class Scalar(base.BaseModel):
    NotFound: ClassVar = abstract.Scalar.NotFound
    NotUnique: ClassVar = abstract.Scalar.NotUnique
    DeletionPrevented: ClassVar = abstract.Scalar.DeletionPrevented

    id: int
    name: str
    value: float
    unit: Unit
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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ScalarDocsRepository(self.backend)

    def create(
        self,
        name: str,
        value: float,
        unit_name: str,
        run_id: int,
    ) -> Scalar:
        return super().create(
            name=name, value=value, unit_name=unit_name, run_id=run_id
        )

    def update(
        self, id: int, value: float | None = None, unit_id: int | None = None
    ) -> Scalar:
        # we can assume this type on update endpoints
        res: Mapping[str, Any] = self._request(
            "PATCH",
            self.prefix + str(id) + "/",
            json={
                "value": value,
                "unit_id": unit_id,
            },
        )  # type: ignore
        return self.model_class(**res)

    def get(self, run_id: int, name: str) -> Scalar:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Scalar:
        res = self._get_by_id(id)
        return Scalar(**res)

    def list(self, *args, **kwargs) -> Iterable[Scalar]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Scalar] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
