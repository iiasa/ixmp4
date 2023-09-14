from datetime import datetime
from typing import ClassVar, Iterable, List

import pandas as pd
from pydantic import StrictInt, StrictStr

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    id: int
    name: str
    elements: StrictInt | list[StrictInt | StrictStr] | StrictStr | None
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
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = IndexSetDocsRepository(self.client)

    def create(
        self,
        run_id: int,
        name: str,
    ) -> IndexSet:
        return super().create(run_id=run_id, name=name)

    def get(self, run_id: int, name: str) -> IndexSet:
        return super().get(name=name, run_id=run_id)

    def list(self, *args, **kwargs) -> Iterable[IndexSet]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[IndexSet] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)

    def add_elements(
        self,
        indexset_id: int,
        elements: StrictInt | List[StrictInt | StrictStr] | StrictStr,
    ) -> None:
        kwargs = {"indexset_id": indexset_id, "elements": elements}
        self._request("PATCH", self.prefix + str(indexset_id) + "/", json=kwargs)
