from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import Session, filters, sql, utils

from .model import Table


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: sql.Select[tuple[Table]], session: Session | None = None
    ) -> sql.Select[tuple[Table]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Table.run__id == Run.id)
        return exc


class OptimizationTableFilter(
    base.OptimizationTableFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: sql.Select[tuple[Table]], session: Session | None = None
    ) -> sql.Select[tuple[Table]]:
        return exc
