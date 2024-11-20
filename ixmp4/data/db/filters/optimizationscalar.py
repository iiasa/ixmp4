from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import Run, Scalar, Unit


class OptimizationScalarFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    run__id: filters.Integer | None = filters.Field(None, alias="run_id")
    unit__id: filters.Integer | None = filters.Field(None, alias="unit_id")

    sqla_model: ClassVar[type] = Scalar

    def join(
        self, exc: sql.Select[tuple[Scalar]], session: Session | None = None
    ) -> sql.Select[tuple[Scalar]]:
        exc = exc.join(Run, onclause=Scalar.run__id == Run.id)
        exc = exc.join(Unit, onclause=Scalar.unit__id == Unit.id)
        return exc
