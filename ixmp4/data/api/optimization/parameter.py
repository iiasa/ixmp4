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


class Parameter(base.BaseModel):
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    id: int
    name: str
    data: dict[str, Any]
    indexset_names: list[str]
    column_names: list[str] | None
    run__id: int

    created_at: datetime | None
    created_by: str | None


class ParameterDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/parameters/"


class ParameterRepository(
    base.Creator[Parameter],
    base.Deleter[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter
    prefix = "optimization/parameters/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = ParameterDocsRepository(self.backend)

    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Parameter:
        return super().create(
            name=name,
            run_id=run_id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    def delete(self, id: int) -> None:
        super().delete(id=id)

    def add_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contain str, not only Hashable
            dict_data = cast(dict[str, Any], data.to_dict(orient="list"))
            data = dict_data
        kwargs = {"data": data}
        self._request(
            method="PATCH", path=self.prefix + str(id) + "/data/", json=kwargs
        )

    def remove_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contain str, not only Hashable
            dict_data = cast(dict[str, Any], data.to_dict(orient="list"))
            data = dict_data
        kwargs = {"data": data}
        self._request(
            method="DELETE", path=self.prefix + str(id) + "/data/", json=kwargs
        )

    def get(self, run_id: int, name: str) -> Parameter:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Parameter:
        res = self._get_by_id(id)
        return Parameter(**res)

    def list(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Parameter]:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._tabulate(json=json)

    def enumerate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Parameter] | pd.DataFrame:
        return super().enumerate(**kwargs)
