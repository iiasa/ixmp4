from collections.abc import Iterable
from datetime import datetime

import pandas as pd

from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.core.base import BaseFacade
from ixmp4.rewrite.data.docs.repository import DocsNotFound
from ixmp4.rewrite.data.scenario.dto import Scenario as ScenarioModel


class Scenario(BaseFacade):
    dto: ScenarioModel

    def __init__(self, backend: Backend, dto: ScenarioModel) -> None:
        super().__init__(backend)
        self.dto = dto

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
            return self._backend.scenarios.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._backend.scenarios.delete_docs(self.id)
        else:
            self._backend.scenarios.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._backend.scenarios.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"


class ScenarioRepository(BaseFacade):
    def create(
        self,
        name: str,
    ) -> Scenario:
        scen = self._backend.scenarios.create(name)
        return Scenario(backend=self._backend, dto=scen)

    def get(self, name: str) -> Scenario:
        scen = self._backend.scenarios.get(name)
        return Scenario(backend=self._backend, dto=scen)

    def list(self, name: str | None = None) -> list[Scenario]:
        scenarios = self._backend.scenarios.list(name=name)
        return [Scenario(backend=self._backend, dto=s) for s in scenarios]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self._backend.scenarios.tabulate(name=name)

    def _get_scenario_id(self, scenario: str) -> int | None:
        # NOTE leaving this check for users without mypy
        if isinstance(scenario, str):
            obj = self._backend.scenarios.get_by_name(scenario)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to scenario: {scenario}")

    def get_docs(self, name: str) -> str | None:
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            return self._backend.scenarios.get_docs(
                dimension__id=scenario_id
            ).description
        except DocsNotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        return self._backend.scenarios.set_docs(
            dimension__id=scenario_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            self._backend.scenarios.delete_docs(dimension__id=scenario_id)
            return None
        except DocsNotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self._backend.scenarios.list_docs(
                dimension__id=id, dimension__id__in=id__in
            )
        ]
