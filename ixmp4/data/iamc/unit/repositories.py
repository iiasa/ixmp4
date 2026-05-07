from typing import Any, Sequence

import sqlalchemy as sa
from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.unit.db import Unit
from ixmp4.data.unit.exceptions import (
    UnitNotFound,
    UnitNotUnique,
)
from ixmp4.data.unit.filter import IamcUnitFilter


class IamcUnitTarget(ModelTarget[Unit]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return super().select_statement(columns=columns).where(Unit.timeseries.any())


class ItemRepository(AuthRepository[Unit], BaseItemRepository[Unit]):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = IamcUnitTarget(Unit)
    filter = Filter(IamcUnitFilter, Unit)


class PandasRepository(AuthRepository[Unit], BasePandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = IamcUnitTarget(Unit)
    filter = Filter(IamcUnitFilter, Unit)
