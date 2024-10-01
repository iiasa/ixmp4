from typing import ClassVar

from ixmp4.db import filters

from .. import Column, Run


class OptimizationColumnFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer = filters.Field(None, alias="run_id")

    # TODO: this should likely be expanded to cover other optimization items, too
    parameter__id: filters.Integer = filters.Field(None, alias="parameter_id")
    table__id: filters.Integer = filters.Field(None, alias="table_id")

    sqla_model: ClassVar[type] = Column

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=Column.run__id == Run.id)
        return exc
