from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Scenario
from .filter import ScenarioFilter


class ScenarioNotFound(NotFound):
    pass


class ScenarioNotUnique(NotUnique):
    pass


class ScenarioDeletionPrevented(DeletionPrevented):
    pass


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
