from ixmp4.db import filters

from .. import IndexSet, Run


class OptimizationIndexSetFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    run__id: filters.Integer = filters.Field(alias="run_id")

    class Config:
        sqla_model = IndexSet

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=IndexSet.run__id == Run.id)
        return exc
