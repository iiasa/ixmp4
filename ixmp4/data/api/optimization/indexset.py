from datetime import datetime
from typing import ClassVar, List

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    id: int
    name: str
    data: float | int | str | list[int | float | str] | None
    run__id: int

    created_at: datetime | None
    created_by: str | None


class IndexSetDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/indexsets/"


class IndexSetRepository(
    base.Creator[IndexSet],
    base.Retriever[IndexSet],
    base.Enumerator[IndexSet],
    abstract.IndexSetRepository,
):
    model_class = IndexSet
    prefix = "optimization/indexsets/"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = IndexSetDocsRepository(self.backend)

    def create(
        self,
        run_id: int,
        name: str,
    ) -> IndexSet:
        return super().create(run_id=run_id, name=name)

    def get(self, run_id: int, name: str) -> IndexSet:
        return super().get(name=name, run_id=run_id)

    def enumerate(self, **kwargs) -> list[IndexSet] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[IndexSet]:
        return super()._list(json=kwargs)

    def tabulate(self, include_data: bool = False, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs, params={"include_data": include_data})

    def add_data(
        self,
        indexset_id: int,
        data: float | int | str | List[float | int | str],
    ) -> None:
        kwargs = {"indexset_id": indexset_id, "data": data}
        self._request("PATCH", self.prefix + str(indexset_id) + "/", json=kwargs)
