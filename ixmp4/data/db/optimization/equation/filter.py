from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import filters, utils

from .model import Equation


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Equation.run__id == Run.id)
        return exc


class EquationFilter(base.OptimizationEquationFilter, metaclass=filters.FilterMeta):
    def join(self, exc, session=None):
        return exc
