from typing import ClassVar

from ixmp4.db import filters

from .. import OptimizationVariable, Run


class OptimizationVariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = OptimizationVariable

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=OptimizationVariable.run__id == Run.id)
        return exc
