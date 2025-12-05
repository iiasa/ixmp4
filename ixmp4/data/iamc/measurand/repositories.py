from toolkit import db

from .db import Measurand, MeasurandVersion
from .exceptions import MeasurandNotFound, MeasurandNotUnique
from .filter import MeasurandFilter, MeasurandVersionFilter


class PandasRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ModelTarget(Measurand)
    filter = db.r.Filter(MeasurandFilter, Measurand)


class VersionRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ModelTarget(MeasurandVersion)
    filter = db.r.Filter(MeasurandVersionFilter, MeasurandVersion)
