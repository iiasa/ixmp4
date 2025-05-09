from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, List, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd
from pydantic import StrictFloat, StrictInt, StrictStr

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    id: int
    name: str
    data: float | int | str | list[int] | list[float] | list[str] | None
    run__id: int

    created_at: datetime | None
    created_by: str | None


class IndexSetDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/indexsets/"


class IndexSetRepository(
    base.Creator[IndexSet],
    base.Deleter[IndexSet],
    base.Retriever[IndexSet],
    base.Enumerator[IndexSet],
    abstract.IndexSetRepository,
):
    model_class = IndexSet
    prefix = "optimization/indexsets/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = IndexSetDocsRepository(self.backend)

    def create(self, run_id: int, name: str) -> IndexSet:
        return super().create(run_id=run_id, name=name)

    def delete(self, id: int) -> None:
        super().delete(id=id)

    def get(self, run_id: int, name: str) -> IndexSet:
        return super().get(name=name, run_id=run_id)

    def enumerate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> list[IndexSet] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> list[IndexSet]:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.optimization.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.OptimizationFilterAlias, kwargs)
        return super()._tabulate(json=json)

    def add_data(
        self,
        id: int,
        data: StrictFloat
        | StrictInt
        | StrictStr
        | List[StrictFloat]
        | List[StrictInt]
        | List[StrictStr],
    ) -> None:
        kwargs = {"id": id, "data": data}
        self._request("PATCH", self.prefix + str(id) + "/data/", json=kwargs)

    def remove_data(
        self,
        id: int,
        data: StrictFloat
        | StrictInt
        | StrictStr
        | List[StrictFloat]
        | List[StrictInt]
        | List[StrictStr],
        remove_dependent_data: bool = True,
    ) -> None:
        kwargs = {"id": id, "data": data}
        self._request(
            "DELETE",
            self.prefix + str(id) + "/data/",
            params={"remove_dependent_data": remove_dependent_data},
            json=kwargs,
        )
