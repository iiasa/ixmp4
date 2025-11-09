from toolkit import db

from ixmp4.rewrite.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Scenario, ScenarioVersion
from .filter import ScenarioFilter


@registry.register()
class ScenarioNotFound(NotFound):
    pass


@registry.register()
class ScenarioNotUnique(NotUnique):
    pass


@registry.register()
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


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = ScenarioNotFound
    NotUnique = ScenarioNotUnique
    target = db.r.ModelTarget(ScenarioVersion)
    filter = db.r.Filter(ScenarioFilter, ScenarioVersion)
