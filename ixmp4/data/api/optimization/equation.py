from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository
from .column import Column


class Equation(base.BaseModel):
    NotFound: ClassVar = abstract.Equation.NotFound
    NotUnique: ClassVar = abstract.Equation.NotUnique
    DeletionPrevented: ClassVar = abstract.Equation.DeletionPrevented

    id: int
    name: str
    data: dict[str, Any]
    columns: list["Column"]
    run__id: int

    created_at: datetime | None
    created_by: str | None


class EquationDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/equations/"


class EquationRepository(
    base.Creator[Equation],
    base.Retriever[Equation],
    base.Enumerator[Equation],
    abstract.EquationRepository,
):
    model_class = Equation
    prefix = "optimization/equations/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = EquationDocsRepository(self.backend)

    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Equation:
        return super().create(
            name=name,
            run_id=run_id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    def add_data(self, equation_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contains str, not only Hashable
            dict_data: dict[str, Any] = data.to_dict(orient="list")  # type: ignore[assignment]
            data = dict_data
        kwargs = {"data": data}
        self._request(
            method="PATCH", path=self.prefix + str(equation_id) + "/data/", json=kwargs
        )

    def remove_data(self, equation_id: int) -> None:
        self._request(method="DELETE", path=self.prefix + str(equation_id) + "/data/")

    def get(self, run_id: int, name: str) -> Equation:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Equation:
        res = self._get_by_id(id)
        return Equation(**res)

    def list(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Equation]:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._tabulate(json=json)

    def enumerate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Equation] | pd.DataFrame:
        return super().enumerate(**kwargs)
