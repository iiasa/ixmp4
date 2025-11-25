from toolkit import db

from ixmp4.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Variable, VariableVersion
from .filter import VariableFilter


@registry.register(name="IamcVariableNotFound")
class VariableNotFound(NotFound):
    pass


@registry.register(name="IamcVariableNotUnique")
class VariableNotUnique(NotUnique):
    pass


@registry.register(name="IamcVariableDeletionPrevented")
class VariableDeletionPrevented(DeletionPrevented):
    pass


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
