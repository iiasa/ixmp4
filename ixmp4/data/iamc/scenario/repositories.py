from typing import Any, Sequence

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.run.db import Run
from ixmp4.data.scenario.db import Scenario
from ixmp4.data.scenario.exceptions import (
    ScenarioNotFound,
    ScenarioNotUnique,
)
from ixmp4.data.scenario.filter import IamcScenarioFilter
from ixmp4.data.scenario.repositories import ScenarioAuthRepository


class IamcScenarioTarget(db.r.ModelTarget[Scenario]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return (
            super()
            .select_statement(columns=columns)
            .where(Scenario.runs.any(Run.timeseries.any()))
        )


class ItemRepository(ScenarioAuthRepository, db.r.ItemRepository[Scenario]):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = IamcScenarioTarget(Scenario)
    filter = db.r.Filter(IamcScenarioFilter, Scenario)


class PandasRepository(ScenarioAuthRepository, db.r.PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = IamcScenarioTarget(Scenario)
    filter = db.r.Filter(IamcScenarioFilter, Scenario)
