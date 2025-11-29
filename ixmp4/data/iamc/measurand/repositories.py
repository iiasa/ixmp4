from toolkit import db

from ixmp4.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import Measurand
from .filter import MeasurandFilter


@registry.register()
class MeasurandNotFound(NotFound):
    pass


@registry.register()
class MeasurandNotUnique(NotUnique):
    pass


@registry.register()
class MeasurandDeletionPrevented(DeletionPrevented):
    pass


class PandasRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ModelTarget(Measurand)
    filter = db.r.Filter(MeasurandFilter, Measurand)
