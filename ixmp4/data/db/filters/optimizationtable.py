from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import Run, Table


class OptimizationTableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = Table

    def join(
        self, exc: sql.Select[tuple[Table]], session: Session | None = None
    ) -> sql.Select[tuple[Table]]:
        exc = exc.join(Run, onclause=Table.run__id == Run.id)
        return exc
