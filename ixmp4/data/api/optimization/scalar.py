from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

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

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
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
        )  # type: ignore[assignment]
        return self.model_class(**res)

    def get(self, run_id: int, name: str) -> Scalar:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Scalar:
        res = self._get_by_id(id)
        return Scalar(**res)

    def list(
        self,
        name: str | None = None,
        **kwargs: Unpack["abstract.optimization.scalar.EnumerateKwargs"],
    ) -> Iterable[Scalar]:
        json = {"name": name, **kwargs}
        return super()._list(json=json)

    def tabulate(
        self,
        name: str | None = None,
        **kwargs: Unpack["abstract.optimization.scalar.EnumerateKwargs"],
    ) -> pd.DataFrame:
        json = {"name": name, **kwargs}
        return super()._tabulate(json=json)

    def enumerate(
        self, **kwargs: Unpack["abstract.optimization.scalar.EnumerateKwargs"]
    ) -> Iterable[Scalar] | pd.DataFrame:
        return super().enumerate(**kwargs)
