from datetime import datetime
from typing import ClassVar

import pandas as pd
from pydantic import Field

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
    abstract.ModelRepository,
):
    model_class = Model
    prefix = "models/"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ModelDocsRepository(self.backend)

    def create(
        self,
        name: str,
    ) -> Model:
        return super().create(name=name)

    def get(self, name: str) -> Model:
        return super().get(name=name)

    def enumerate(self, **kwargs) -> list[Model] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[Model]:
        return super()._list(json=kwargs)

    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs)


class ModelDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/models/"
