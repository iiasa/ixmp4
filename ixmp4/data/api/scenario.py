from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from ixmp4.data.backend.api import RestBackend

import pandas as pd
from pydantic import Field

# TODO Import this from typing when dropping support for Python 3.11
from typing_extensions import Unpack

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class Scenario(base.BaseModel):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    id: int
    name: str
    created_at: datetime | None = Field(None)
    created_by: str | None = Field(None)


class ScenarioDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/scenarios/"


class ScenarioRepository(
    base.Creator[Scenario],
    base.Retriever[Scenario],
    base.Enumerator[Scenario],
    abstract.ScenarioRepository,
):
    model_class = Scenario
    prefix = "scenarios/"

    def __init__(self, *args: Unpack[tuple["RestBackend"]]) -> None:
        super().__init__(*args)
        self.docs = ScenarioDocsRepository(self.backend)

    def create(
        self,
        name: str,
    ) -> Scenario:
        return super().create(name=name)

    def get(self, name: str) -> Scenario:
        return super().get(name=name)

    def enumerate(
        self, **kwargs: Unpack[abstract.scenario.EnumerateKwargs]
    ) -> list[Scenario] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(
        self,
        name: str | None = None,
        **kwargs: Unpack[abstract.scenario.EnumerateKwargs],
    ) -> list[Scenario]:
        json = {"name": name, **kwargs}
        return super()._list(json=json)

    def tabulate(
        self,
        name: str | None = None,
        **kwargs: Unpack[abstract.scenario.EnumerateKwargs],
    ) -> pd.DataFrame:
        json = {"name": name, **kwargs}
        return super()._tabulate(json=json)
