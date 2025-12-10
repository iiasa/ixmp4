from typing import TYPE_CHECKING

from toolkit import db

from .db import Unit, UnitVersion
from .exceptions import UnitNotFound, UnitNotUnique
from .filter import UnitFilter

if TYPE_CHECKING:
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


class VersionRepository(db.r.PandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = db.r.ModelTarget(UnitVersion)
    filter = db.r.Filter(UnitFilter, UnitVersion)
