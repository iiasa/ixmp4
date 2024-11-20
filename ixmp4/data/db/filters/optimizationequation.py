from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import Equation, Run


class OptimizationEquationFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = Equation

    def join(
        self, exc: sql.Select[tuple[Equation]], session: Session | None = None
    ) -> sql.Select[tuple[Equation]]:
        exc = exc.join(Run, onclause=Equation.run__id == Run.id)
        return exc
