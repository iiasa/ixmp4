from toolkit import db

from .db import Variable, VariableVersion
from .exceptions import VariableNotFound, VariableNotUnique
from .filter import VariableFilter


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


class VersionPandasRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(VariableVersion)
