from typing import ClassVar

from ixmp4.db import filters

from .. import Run, Scalar, Unit


class OptimizationScalarFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer = filters.Field(None, alias="run_id")
    unit__id: filters.Integer = filters.Field(None, alias="unit_id")

    sqla_model: ClassVar[type] = Scalar

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=Scalar.run__id == Run.id)
        exc = exc.join(Unit, onclause=Scalar.unit__id == Unit.id)
        return exc
