from typing import Any, Sequence

import sqlalchemy as sa
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.region.db import Region
from ixmp4.data.region.exceptions import (
    RegionNotFound,
    RegionNotUnique,
)
from ixmp4.data.region.filter import IamcRegionFilter


class IamcRegionTarget(ModelTarget[Region]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return super().select_statement(columns=columns).where(Region.timeseries.any())


class ItemRepository(AuthRepository[Region], BaseItemRepository[Region]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = IamcRegionTarget(Region)
    filter = Filter(IamcRegionFilter, Region)


class PandasRepository(AuthRepository[Region], BasePandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = IamcRegionTarget(Region)
    filter = Filter(IamcRegionFilter, Region)
