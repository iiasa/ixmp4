from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class Model(base.BaseModel):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    id: int
    name: str

    created_at: datetime | None = Field(None)
    created_by: str | None = Field(None)


class ModelRepository(
    base.Creator[Model],
    base.Retriever[Model],
    base.Enumerator[Model],
    base.VersionManager[Model],
    abstract.ModelRepository,
):
    model_class = Model
    prefix = "models/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = ModelDocsRepository(self.backend)

    def create(self, name: str) -> Model:
        return super().create(name=name)

    def get(self, name: str) -> Model:
        return super().get(name=name)

    def enumerate(
        self, **kwargs: Unpack[abstract.model.EnumerateKwargs]
    ) -> list[Model] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs: Unpack[abstract.model.EnumerateKwargs]) -> list[Model]:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.model.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.IamcObjectFilterAlias, kwargs)
        return super()._tabulate(json=json)


class ModelDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/models/"
