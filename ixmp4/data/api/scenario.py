from datetime import datetime
from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from . import base
from .docs import Docs, DocsRepository


class Scenario(base.BaseModel):
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    id: int
    name: str
    created_at: datetime | None
    created_by: str | None


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
    enumeration_method = "PATCH"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = ScenarioDocsRepository(self.client)

    def create(
        self,
        name: str,
    ) -> Scenario:
        return super().create(name=name)

    def get(self, name: str) -> Scenario:
        return super().get(name=name)

    def list(self, *args, **kwargs) -> Iterable[Scenario]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Scenario] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)
