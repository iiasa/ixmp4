from typing import ClassVar

from ixmp4.db import filters, sql

from .. import Column, Run


class OptimizationColumnFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = Column

    # Not fixing this since I think we don't need columns
    def join(self, exc: sql.Select, **kwargs) -> sql.Select:  # type: ignore[no-untyped-def,type-arg]
        exc = exc.join(Run, onclause=Column.run__id == Run.id)  # type: ignore[attr-defined]
        return exc
