from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class Model(base.BaseModel):
    NotFound: ClassVar = abstract.Model.NotFound
    NotUnique: ClassVar = abstract.Model.NotUnique
    DeletionPrevented: ClassVar = abstract.Model.DeletionPrevented

    id: int
    name: str

    created_at: datetime | None
    created_by: str | None


class ModelRepository(
    base.Creator[Model],
    base.Retriever[Model],
    base.Enumerator[Model],
    abstract.ModelRepository,
):
    model_class = Model
    prefix = "models/"
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = ModelDocsRepository(self.client)

    def create(
        self,
        name: str,
    ) -> Model:
        return super().create(name=name)

    def get(self, name: str) -> Model:
        return super().get(name=name)

    def list(self, *args, **kwargs) -> Iterable[Model]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Model] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)


class ModelDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/models/"
