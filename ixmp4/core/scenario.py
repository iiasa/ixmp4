from datetime import datetime
from typing import ClassVar

import pandas as pd

from ixmp4.core.base import BaseFacade, BaseModelFacade
from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Scenario as ScenarioModel


class Scenario(BaseModelFacade):
    _model: ScenarioModel
    NotFound: ClassVar = ScenarioModel.NotFound
    NotUnique: ClassVar = ScenarioModel.NotUnique

    @property
    def id(self) -> int:
        return self._model.id

    @property
    def name(self) -> str:
        return self._model.name

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    @property
    def docs(self):
        try:
            return self.backend.scenarios.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.scenarios.docs.delete(self.id)
        else:
            self.backend.scenarios.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.scenarios.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Scenario {self.id} name={self.name}>"


class ScenarioRepository(BaseFacade):
    def create(
        self,
        name: str,
    ) -> Scenario:
        model = self.backend.scenarios.create(name)
        return Scenario(_backend=self.backend, _model=model)

    def get(self, name: str) -> Scenario:
        model = self.backend.scenarios.get(name)
        return Scenario(_backend=self.backend, _model=model)

    def list(self, name: str | None = None) -> list[Scenario]:
        scenarios = self.backend.scenarios.list(name=name)
        return [Scenario(_backend=self.backend, _model=s) for s in scenarios]

    def tabulate(self, name: str | None = None) -> pd.DataFrame:
        return self.backend.scenarios.tabulate(name=name)

    def _get_scenario_id(self, scenario: str) -> int | None:
        if scenario is None:
            return None
        elif isinstance(scenario, str):
            obj = self.backend.scenarios.get(scenario)
            return obj.id
        else:
            raise ValueError(f"Invalid reference to scenario: {scenario}")

    def get_docs(self, name: str) -> str | None:
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            return self.backend.scenarios.docs.get(dimension_id=scenario_id).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        return self.backend.scenarios.docs.set(
            dimension_id=scenario_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        scenario_id = self._get_scenario_id(name)
        if scenario_id is None:
            return None
        try:
            self.backend.scenarios.docs.delete(dimension_id=scenario_id)
            return None
        except DocsModel.NotFound:
            return None
