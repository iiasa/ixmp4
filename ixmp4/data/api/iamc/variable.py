from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, cast

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

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
    base.VersionManager[Variable],
    abstract.VariableRepository,
):
    model_class = Variable
    prefix = "iamc/variables/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = VariableDocsRepository(self.backend)

    def create(
        self,
        name: str,
    ) -> Variable:
        return super().create(name=name)

    def get(self, name: str) -> Variable:
        return super().get(name=name)

    def enumerate(
        self, **kwargs: Unpack[abstract.iamc.variable.EnumerateKwargs]
    ) -> list[Variable] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self, **kwargs: Unpack[abstract.iamc.variable.EnumerateKwargs]
    ) -> list[Variable]:
        json = cast(abstract.annotations.IamcFilterAlias, kwargs)
        return super()._list(json=json)

    def tabulate(
        self, **kwargs: Unpack[abstract.iamc.variable.EnumerateKwargs]
    ) -> pd.DataFrame:
        json = cast(abstract.annotations.IamcFilterAlias, kwargs)
        return super()._tabulate(json=json)
