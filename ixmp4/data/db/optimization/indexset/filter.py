from ixmp4.data.db import filters as base
from ixmp4.data.db.run import Run
from ixmp4.db import Session, filters, sql, utils

from .model import IndexSet, IndexSetData


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: sql.Select[tuple[IndexSet]], session: Session | None = None
    ) -> sql.Select[tuple[IndexSet]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=IndexSet.run__id == Run.id)
        return exc


class OptimizationIndexSetFilter(
    base.OptimizationIndexSetFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: sql.Select[tuple[IndexSet]], session: Session | None = None
    ) -> sql.Select[tuple[IndexSet]]:
        return exc


class OptimizationIndexSetDataFilter(
    base.OptimizationIndexSetDataFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: sql.Select[tuple[IndexSetData]], session: Session | None = None
    ) -> sql.Select[tuple[IndexSetData]]:
        return exc
