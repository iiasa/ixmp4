from datetime import datetime
from typing import ClassVar

import pandas as pd
from pydantic import Field

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class Variable(base.BaseModel):
    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    id: int
    name: str

    created_at: datetime | None = Field(None)
    created_by: str | None = Field(None)


class VariableDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/iamc/variables/"


class VariableRepository(
    base.Creator[Variable],
    base.Retriever[Variable],
    base.Enumerator[Variable],
    abstract.VariableRepository,
):
    model_class = Variable
    prefix = "iamc/variables/"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = VariableDocsRepository(self.backend)

    def create(
        self,
        name: str,
    ) -> Variable:
        return super().create(name=name)

    def get(self, name: str) -> Variable:
        return super().get(name=name)

    def enumerate(self, **kwargs) -> list[Variable] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[Variable]:
        return super()._list(json=kwargs)

    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs)
