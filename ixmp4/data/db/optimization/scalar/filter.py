from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.data.db.unit import Unit
from ixmp4.db import filters, utils

from .model import Scalar


class OptimizationUnitFilter(base.UnitFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwawrgs):
        if not utils.is_joined(exc, Unit):
            exc = exc.join(Unit, onclause=Scalar.unit__id == Unit.id)
        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Scalar.run__id == Run.id)
        return exc


class OptimizationScalarFilter(
    base.OptimizationScalarFilter, metaclass=filters.FilterMeta
):
    def join(self, exc, session=None):
        return exc
