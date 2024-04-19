from datetime import datetime
from typing import ClassVar, List

import pandas as pd
from pydantic import StrictFloat, StrictInt, StrictStr

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    id: int
    name: str
    elements: (
        StrictFloat
        | StrictInt
        | StrictStr
        | list[StrictFloat | StrictInt | StrictStr]
        | None
    )
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

    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs)

    def add_elements(
        self,
        indexset_id: int,
        elements: StrictFloat
        | StrictInt
        | List[StrictFloat | StrictInt | StrictStr]
        | StrictStr,
    ) -> None:
        kwargs = {"indexset_id": indexset_id, "elements": elements}
        self._request("PATCH", self.prefix + str(indexset_id) + "/", json=kwargs)
