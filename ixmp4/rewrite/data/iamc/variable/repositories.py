from toolkit import db

from ixmp4.rewrite.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Variable


@registry.register()
class VariableNotFound(NotFound):
    pass


@registry.register()
class VariableNotUnique(NotUnique):
    pass


@registry.register()
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
