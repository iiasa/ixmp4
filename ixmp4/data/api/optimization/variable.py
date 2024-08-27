from datetime import datetime
from typing import Any, ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository
from .column import Column


class Variable(base.BaseModel):
    NotFound: ClassVar = abstract.OptimizationVariable.NotFound
    NotUnique: ClassVar = abstract.OptimizationVariable.NotUnique
    DeletionPrevented: ClassVar = abstract.OptimizationVariable.DeletionPrevented

    id: int
    name: str
    data: dict[str, Any]
    columns: list["Column"]
    run__id: int

    created_at: datetime | None
    created_by: str | None


class VariableDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/variables/"


class VariableRepository(
    base.Creator[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    abstract.OptimizationVariableRepository,
):
    model_class = Variable
    prefix = "optimization/variables/"

    def __init__(self, backend, *args, **kwargs) -> None:
        super().__init__(backend, *args, **kwargs)
        self.docs = VariableDocsRepository(backend)

    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: str | list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        return super().create(
            name=name,
            run_id=run_id,
            constrained_to_indexsets=constrained_to_indexsets,
            column_names=column_names,
        )

    def add_data(self, variable_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        if isinstance(data, pd.DataFrame):
            # data will always contains str, not only Hashable
            data: dict[str, Any] = data.to_dict(orient="list")  # type: ignore
        kwargs = {"data": data}
        self._request(
            method="PATCH", path=self.prefix + str(variable_id) + "/data/", json=kwargs
        )

    def remove_data(self, variable_id: int) -> None:
        self._request(method="DELETE", path=self.prefix + str(variable_id) + "/data/")

    def get(self, run_id: int, name: str) -> Variable:
        return super().get(run_id=run_id, name=name)

    def get_by_id(self, id: int) -> Variable:
        res = self._get_by_id(id)
        return Variable(**res)

    def list(self, *args, **kwargs) -> Iterable[Variable]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Variable] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
