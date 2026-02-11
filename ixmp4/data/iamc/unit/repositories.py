from typing import Any, Sequence

import sqlalchemy as sa
from toolkit import db

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.unit.db import Unit
from ixmp4.data.unit.exceptions import (
    UnitNotFound,
    UnitNotUnique,
)
from ixmp4.data.unit.filter import IamcUnitFilter


class IamcUnitTarget(db.r.ModelTarget[Unit]):
    def select_statement(
        self, columns: Sequence[str] | None = None
    ) -> sa.Select[tuple[Any, ...]]:
        return super().select_statement(columns=columns).where(Unit.timeseries.any())


class ItemRepository(AuthRepository[Unit], db.r.ItemRepository[Unit]):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = IamcUnitTarget(Unit)
    filter = db.r.Filter(IamcUnitFilter, Unit)


class PandasRepository(AuthRepository[Unit], db.r.PandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = IamcUnitTarget(Unit)
    filter = db.r.Filter(IamcUnitFilter, Unit)
