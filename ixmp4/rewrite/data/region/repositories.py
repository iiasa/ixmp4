from toolkit import db

from ixmp4.rewrite.exceptions import (
    DeletionPrevented,
    NotFound,
    NotUnique,
    registry,
)

from .db import Region
from .filter import RegionFilter


@registry.register()
class RegionNotFound(NotFound):
    pass


@registry.register()
class RegionNotUnique(NotUnique):
    pass


@registry.register()
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
