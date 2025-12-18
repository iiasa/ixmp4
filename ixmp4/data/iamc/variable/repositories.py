from toolkit import db

from .db import Variable, VariableVersion
from .exceptions import VariableNotFound, VariableNotUnique
from .filter import VariableFilter, VariableVersionFilter


class ItemRepository(db.r.ItemRepository[Variable]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(Variable)
    filter = db.r.Filter(VariableFilter, Variable)


class PandasRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(Variable)
    filter = db.r.Filter(VariableFilter, Variable)


class VersionRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    filter = db.r.Filter(VariableVersionFilter, VariableVersion)
    target = db.r.ModelTarget(VariableVersion)
