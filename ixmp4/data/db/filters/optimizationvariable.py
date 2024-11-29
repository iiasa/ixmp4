from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import OptimizationVariable, Run


class OptimizationVariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = OptimizationVariable

    def join(
        self,
        exc: sql.Select[tuple[OptimizationVariable]],
        session: Session | None = None,
    ) -> sql.Select[tuple[OptimizationVariable]]:
        exc = exc.join(Run, onclause=OptimizationVariable.run__id == Run.id)
        return exc
