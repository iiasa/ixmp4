from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from .db import Region, RegionVersion
from .exceptions import RegionNotFound, RegionNotUnique
from .filter import RegionFilter, RegionVersionFilter


class ItemRepository(BaseItemRepository[Region | RegionVersion]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = ModelTarget(Region)
    filter = Filter(RegionFilter, Region)


class PandasRepository(BasePandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = ModelTarget(Region)
    filter = Filter(RegionFilter, Region)


class VersionRepository(PandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = ModelTarget(RegionVersion)
    filter = Filter(RegionVersionFilter, RegionVersion)
