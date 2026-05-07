from toolkit.db.filter import Filter
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.target import ModelTarget

from .db import Measurand, MeasurandVersion
from .exceptions import MeasurandNotFound, MeasurandNotUnique
from .filter import MeasurandFilter, MeasurandVersionFilter


class PandasRepository(BasePandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = ModelTarget(Measurand)
    filter = Filter(MeasurandFilter, Measurand)


class VersionRepository(BasePandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = ModelTarget(MeasurandVersion)
    filter = Filter(MeasurandVersionFilter, MeasurandVersion)
