from toolkit import db

from .db import Scenario, ScenarioVersion
from .exceptions import ScenarioNotFound, ScenarioNotUnique
from .filter import ScenarioFilter


class ItemRepository(db.r.ItemRepository[Scenario]):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(Scenario)
    filter = db.r.Filter(ScenarioFilter, Scenario)


class PandasRepository(db.r.PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(Scenario)
    filter = db.r.Filter(ScenarioFilter, Scenario)


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(ScenarioVersion)
    filter = db.r.Filter(ScenarioFilter, ScenarioVersion)
