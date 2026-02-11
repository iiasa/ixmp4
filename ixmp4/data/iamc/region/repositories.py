from typing import Any, Sequence

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.region.db import Region
from ixmp4.data.region.exceptions import (
    RegionNotFound,
    RegionNotUnique,
)
from ixmp4.data.region.filter import IamcRegionFilter


class IamcRegionTarget(db.r.ModelTarget[Region]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return super().select_statement(columns=columns).where(Region.timeseries.has())


class ItemRepository(AuthRepository[Region], db.r.ItemRepository[Region]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = IamcRegionTarget(Region)
    filter = db.r.Filter(IamcRegionFilter, Region)


class PandasRepository(AuthRepository[Region], db.r.PandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = IamcRegionTarget(Region)
    filter = db.r.Filter(IamcRegionFilter, Region)
