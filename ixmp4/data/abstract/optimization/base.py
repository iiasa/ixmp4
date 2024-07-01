from typing import Any, Generic, Iterable, Protocol, TypeVar

import pandas as pd

# from ixmp4.data import types
from .. import base
from ..docs import DocsRepository

# from .column import Column


# TODO Currently not in use
# class OptimizationBaseModel(base.BaseModel, Protocol):
#     id: types.Integer
#     name: types.String
#     data: types.JsonDict
#     columns: types.Mapped[list[Column]]
#     run__id: types.Integer
#     created_at: types.DateTime
#     created_by: types.String


BackendModelType = TypeVar("BackendModelType", bound=base.BaseModel, covariant=True)


class BackendBaseRepository(
    Generic[BackendModelType],
    base.Creator,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(
        self,
        run_id: int,
        name: str,
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> BackendModelType: ...

    def get(self, run_id: int, name: str) -> BackendModelType: ...

    def get_by_id(self, id: int) -> BackendModelType: ...

    def list(
        self, *, name: str | None = None, **kwargs
    ) -> Iterable[BackendModelType]: ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame: ...

    def add_data(self, table_id: int, data: dict[str, Any] | pd.DataFrame) -> None: ...
