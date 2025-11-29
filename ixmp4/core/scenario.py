from collections.abc import Iterable
from datetime import datetime

import pandas as pd

from ixmp4.core.base import BaseFacadeObject, BaseServiceFacade
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.scenario.dto import Scenario as ScenarioDto
from ixmp4.data.scenario.exceptions import (
    ScenarioDeletionPrevented,
    ScenarioNotFound,
    ScenarioNotUnique,
)
from ixmp4.data.scenario.service import ScenarioService


class Scenario(BaseFacadeObject[ScenarioService, ScenarioDto]):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    DeletionPrevented = ScenarioDeletionPrevented

    @property
    def id(self) -> int:
        return self.dto.id

    @property
    def name(self) -> str:
        return self.dto.name

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self.service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self.service.delete_docs(self.id)
        else:
            self.service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self.service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"


class ScenarioServiceFacade(BaseServiceFacade[ScenarioService]):
    def create(
        self,
        name: str,
    ) -> Scenario:
        scen = self.service.create(name)
        return Scenario(backend=self._backend, dto=scen)

    def get(self, name: str) -> Scenario:
        scen = self.service.get_by_name(name)
        return Scenario(self.service, scen)

    def list(self, name: str | None = None) -> list[Scenario]:
        scenarios = self.service.list(name=name)
        return [Scenario(self.service, s) for s in scenarios]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.service.tabulate(name=name)

    def _get_scenario_id(self, scenario: str) -> int | None:
        # NOTE leaving this check for users without mypy
        if isinstance(scenario, str):
            obj = self.service.get_by_name(scenario)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to scenario: {scenario}")

    def get_docs(self, name: str) -> str | None:
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            return self.service.get_docs(dimension__id=scenario_id).description
        except DocsNotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        return self.service.set_docs(
            dimension__id=scenario_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            self.service.delete_docs(dimension__id=scenario_id)
            return None
        except DocsNotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self.service.list_docs(
                dimension__id=id, dimension__id__in=id__in
            )
        ]
