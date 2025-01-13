from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import IndexSet, Run


class OptimizationIndexSetFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = IndexSet

    def join(
        self, exc: sql.Select[tuple[IndexSet]], session: Session | None = None
    ) -> sql.Select[tuple[IndexSet]]:
        exc = exc.join(Run, onclause=IndexSet.run__id == Run.id)
        return exc
