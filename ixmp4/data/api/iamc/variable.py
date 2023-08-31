from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from .. import base
from ..docs import Docs, DocsRepository


class Variable(base.BaseModel):
    NotFound: ClassVar = abstract.Variable.NotFound
    NotUnique: ClassVar = abstract.Variable.NotUnique
    DeletionPrevented: ClassVar = abstract.Variable.DeletionPrevented

    id: int
    name: str

    created_at: datetime | None
    created_by: str | None


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
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = VariableDocsRepository(self.client)

    def create(
        self,
        name: str,
    ) -> Variable:
        return super().create(name=name)

    def get(self, name: str) -> Variable:
        return super().get(name=name)

    def list(self, *args, **kwargs) -> Iterable[Variable]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Variable] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
