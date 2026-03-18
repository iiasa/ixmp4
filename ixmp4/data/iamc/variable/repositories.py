from toolkit.db.filter import Filter
from toolkit.db.repositories import ItemRepository as BaseItemRepository
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from .db import Variable, VariableVersion
from .exceptions import VariableNotFound, VariableNotUnique
from .filter import VariableFilter, VariableVersionFilter


class ItemRepository(BaseItemRepository[Variable | VariableVersion]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = ModelTarget(Variable)
    filter = Filter(VariableFilter, Variable)


class PandasRepository(BasePandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = ModelTarget(Variable)
    filter = Filter(VariableFilter, Variable)


class VersionRepository(PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    filter = Filter(VariableVersionFilter, VariableVersion)
    target = ModelTarget(VariableVersion)
