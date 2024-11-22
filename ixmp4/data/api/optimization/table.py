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


class Table(base.BaseModel):
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    id: int
    name: str
    data: dict[str, Any]
    columns: list["Column"]
    run__id: int

    created_at: datetime | None
    created_by: str | None


class TableDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/tables/"


class TableRepository(
    base.Creator[Table],
    base.Retriever[Table],
    base.Enumerator[Table],
    abstract.TableRepository,
):
    model_class = Table
    prefix = "optimization/tables/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = TableDocsRepository(self.backend)

    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        return super().create(
            name=name,
            run_id=run_id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    def add_data(self, table_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contains str, not only Hashable
            dict_data: dict[str, Any] = data.to_dict(orient="list")  # type: ignore[assignment]
            data = dict_data
        kwargs = {"data": data}
        self._request(
            method="PATCH", path=self.prefix + str(table_id) + "/data/", json=kwargs
        )

    def get(self, run_id: int, name: str) -> Table:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Table:
        res = self._get_by_id(id)
        return Table(**res)

    def list(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Table]:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._tabulate(json=json)

    def enumerate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> Iterable[Table] | pd.DataFrame:
        return super().enumerate(**kwargs)
