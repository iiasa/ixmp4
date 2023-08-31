from typing import ClassVar, Iterable

import pandas as pd

from ixmp4.data import abstract

from . import base
from .model import Model
from .scenario import Scenario


class Run(base.BaseModel):
    NotFound: ClassVar = abstract.Run.NotFound
    NotUnique: ClassVar = abstract.Run.NotUnique
    DeletionPrevented: ClassVar = abstract.Run.DeletionPrevented

    NoDefaultVersion: ClassVar = abstract.Run.NoDefaultVersion

    id: int

    model: Model
    model__id: int

    scenario: Scenario
    scenario__id: int

    version: int
    is_default: bool


class RunRepository(
    base.Creator[Run],
    base.Retriever[Run],
    base.Enumerator[Run],
    abstract.RunRepository,
):
    model_class = Run
    prefix = "runs/"
    enumeration_method = "PATCH"

    def create(self, model_name: str, scenario_name: str) -> Run:
        return super().create(model_name=model_name, scenario_name=scenario_name)

    def get(self, model_name: str, scenario_name: str, version: int) -> Run:
        return super().get(
            model={"name": model_name},
            scenario={"name": scenario_name},
            version=version,
            default_only=False,
        )

    def list(self, *args, **kwargs) -> Iterable[Run]:
        return super().list(*args, **kwargs)

    def tabulate(self, *args, **kwargs) -> pd.DataFrame:
        return super().tabulate(*args, **kwargs)

    def enumerate(self, *args, **kwargs) -> Iterable[Run] | pd.DataFrame:
        return super().enumerate(*args, **kwargs)

    def get_default_version(self, model_name: str, scenario_name: str) -> Run:
        try:
            return super().get(
                model={"name": model_name},
                scenario={"name": scenario_name},
                is_default=True,
            )
        except Run.NotFound:
            raise Run.NoDefaultVersion

    def set_as_default_version(self, id: int) -> None:
        self._request(
            "POST",
            self.prefix + "/".join([str(id), "set-as-default-version/"]),
        )

    def unset_as_default_version(self, id: int) -> None:
        self._request(
            "POST",
            self.prefix + "/".join([str(id), "unset-as-default-version/"]),
        )
