from typing import TYPE_CHECKING

from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from .db import Unit, UnitVersion
from .exceptions import UnitNotFound, UnitNotUnique
from .filter import UnitFilter, UnitVersionFilter

if TYPE_CHECKING:
    pass


class ItemRepository(BaseItemRepository[Unit]):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = ModelTarget(Unit)
    filter = Filter(UnitFilter, Unit)


class PandasRepository(BasePandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target: ModelTarget[Unit | UnitVersion] = ModelTarget(Unit)
    filter = Filter(UnitFilter, Unit)


class VersionRepository(PandasRepository):
    NotFound = UnitNotFound
    NotUnique = UnitNotUnique
    target = ModelTarget(UnitVersion)
    filter = Filter(UnitVersionFilter, UnitVersion)
