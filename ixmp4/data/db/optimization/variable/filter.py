from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import filters, utils

from .model import OptimizationVariable as Variable


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Variable.run__id == Run.id)
        return exc


class OptimizationVariableFilter(
    base.OptimizationVariableFilter, metaclass=filters.FilterMeta
):
    def join(self, exc, session=None):
        return exc
