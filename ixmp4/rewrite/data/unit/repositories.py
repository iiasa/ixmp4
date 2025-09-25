from typing import TYPE_CHECKING

from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Unit
from .filter import UnitFilter

if TYPE_CHECKING:
    pass


class UnitNotFound(NotFound):
    pass


class UnitNotUnique(NotUnique):
    pass


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
