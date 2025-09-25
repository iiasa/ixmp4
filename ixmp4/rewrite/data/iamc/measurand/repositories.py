from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import Measurand


class MeasurandNotFound(NotFound):
    pass


class MeasurandNotUnique(NotUnique):
    pass


class MeasurandDeletionPrevented(DeletionPrevented):
    pass


class PandasRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ModelTarget(Measurand)
