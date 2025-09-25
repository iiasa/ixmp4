from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Variable


class VariableNotFound(NotFound):
    pass


class VariableNotUnique(NotUnique):
    pass


class VariableDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[Variable]):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(Variable)


class PandasRepository(db.r.PandasRepository):
    NotFound = VariableNotFound
    NotUnique = VariableNotUnique
    target = db.r.ModelTarget(Variable)
