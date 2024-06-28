from datetime import datetime
from typing import Any, ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository
from .column import Column


class Parameter(base.BaseModel):
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    id: int
    name: str
    data: dict[str, Any]
    columns: list["Column"]
    run__id: int

    created_at: datetime | None
    created_by: str | None


class ParameterDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/parameters/"


class ParameterRepository(
    base.Creator[Parameter],
    base.Retriever[Parameter],
    base.Enumerator[Parameter],
    abstract.ParameterRepository,
):
    model_class = Parameter
    prefix = "optimization/parameters/"

    def __init__(self, backend, *args, **kwargs) -> None:
        super().__init__(backend, *args, **kwargs)
        self.docs = ParameterDocsRepository(backend)

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

    def add_data(self, parameter_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contains str, not only Hashable
            data: dict[str, Any] = data.to_dict(orient="list")  # type: ignore
        kwargs = {"data": data}
        self._request(
            method="PATCH", path=self.prefix + str(parameter_id) + "/data/", json=kwargs
        )

    def get(self, run_id: int, name: str) -> Parameter:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Parameter:
        res = self._get_by_id(id)
        return Parameter(**res)

    def list(self, *args, **kwargs) -> Iterable[Parameter]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Parameter] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
