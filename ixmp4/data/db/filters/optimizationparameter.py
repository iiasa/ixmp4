from typing import ClassVar

from ixmp4.db import filters

from .. import Parameter, Run


class OptimizationParameterFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer = filters.Field(None, alias="run_id")

    sqla_model: ClassVar[type] = Parameter

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=Parameter.run__id == Run.id)
        return exc
