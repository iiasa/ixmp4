from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import Session, filters, sql, utils

from .model import Parameter


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: sql.Select[tuple[Parameter]], session: Session | None = None
    ) -> sql.Select[tuple[Parameter]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Parameter.run__id == Run.id)
        return exc


class OptimizationParameterFilter(
    base.OptimizationParameterFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: sql.Select[tuple[Parameter]], session: Session | None = None
    ) -> sql.Select[tuple[Parameter]]:
        return exc
