from typing import TYPE_CHECKING

from toolkit import db

from ixmp4.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Unit, UnitVersion
from .filter import UnitFilter

if TYPE_CHECKING:
    pass


@registry.register()
class UnitNotFound(NotFound):
    pass


@registry.register()
class UnitNotUnique(NotUnique):
    pass


@registry.register()
class UnitDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Unit]):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = db.r.ModelTarget(Unit)
    filter = db.r.Filter(UnitFilter, Unit)


class PandasRepository(db.r.PandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = db.r.ModelTarget(Unit)
    filter = db.r.Filter(UnitFilter, Unit)


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = db.r.ModelTarget(UnitVersion)
    filter = db.r.Filter(UnitFilter, UnitVersion)
