from datetime import datetime
from typing import ClassVar

import pandas as pd
from pydantic import Field

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

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ScenarioDocsRepository(self.backend)

    def create(
        self,
        name: str,
    ) -> Scenario:
        return super().create(name=name)

    def get(self, name: str) -> Scenario:
        return super().get(name=name)

    def enumerate(self, **kwargs) -> list[Scenario] | pd.DataFrame:
        return super().enumerate(**kwargs)

    def list(self, **kwargs) -> list[Scenario]:
        return super()._list(json=kwargs)

    def tabulate(self, **kwargs) -> pd.DataFrame:
        return super()._tabulate(json=kwargs)
