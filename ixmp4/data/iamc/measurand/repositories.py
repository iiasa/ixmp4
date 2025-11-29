from toolkit import db

from .db import Measurand
from .exceptions import MeasurandNotFound, MeasurandNotUnique
from .filter import MeasurandFilter


class PandasRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ModelTarget(Measurand)
    filter = db.r.Filter(MeasurandFilter, Measurand)
