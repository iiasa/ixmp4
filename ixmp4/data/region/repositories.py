from toolkit import db

from .db import Region, RegionVersion
from .exceptions import RegionNotFound, RegionNotUnique
from .filter import RegionFilter


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


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = db.r.ModelTarget(RegionVersion)
    filter = db.r.Filter(RegionFilter, RegionVersion)
