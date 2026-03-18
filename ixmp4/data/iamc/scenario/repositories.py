from typing import Any, Sequence

import sqlalchemy as sa
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.run.db import Run
from ixmp4.data.scenario.db import Scenario, ScenarioVersion
from ixmp4.data.scenario.exceptions import (
    ScenarioNotFound,
    ScenarioNotUnique,
)
from ixmp4.data.scenario.filter import IamcScenarioFilter
from ixmp4.data.scenario.repositories import ScenarioAuthRepository


class IamcScenarioTarget(ModelTarget[Scenario | ScenarioVersion]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return (
            super()
            .select_statement(columns=columns)
            .where(Scenario.runs.any(Run.timeseries.any()))
        )


class ItemRepository(ScenarioAuthRepository, BaseItemRepository[Scenario]):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = IamcScenarioTarget(Scenario)
    filter = Filter(IamcScenarioFilter, Scenario)


class PandasRepository(ScenarioAuthRepository, BasePandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = IamcScenarioTarget(Scenario)
    filter = Filter(IamcScenarioFilter, Scenario)
