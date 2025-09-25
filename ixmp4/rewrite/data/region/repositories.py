from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Region
from .filter import RegionFilter


class RegionNotFound(NotFound):
    pass


class RegionNotUnique(NotUnique):
    pass


class RegionDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Region]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = db.r.ModelTarget(Region)
    filter = db.r.Filter(RegionFilter, Region)


class PandasRepository(db.r.PandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = db.r.ModelTarget(Region)
    filter = db.r.Filter(RegionFilter, Region)
