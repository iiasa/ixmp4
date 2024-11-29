from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import Session, filters, sql, utils

from .model import OptimizationVariable as Variable


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: sql.Select[tuple[Variable]], session: Session | None = None
    ) -> sql.Select[tuple[Variable]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Variable.run__id == Run.id)
        return exc


class OptimizationVariableFilter(
    base.OptimizationVariableFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: sql.Select[tuple[Variable]], session: Session | None = None
    ) -> sql.Select[tuple[Variable]]:
        return exc
