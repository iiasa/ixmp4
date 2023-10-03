from typing_extensions import Annotated

from ixmp4.db import filters

from .. import IndexSet, Run


class OptimizationIndexSetFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    name: Annotated[filters.String | None, filters.Field(None)]
    run__id: Annotated[filters.Integer | None, filters.Field(None, alias="run_id")]

    _sqla_model = IndexSet

    def join(self, exc, **kwargs):
        exc = exc.join(Run, onclause=IndexSet.run__id == Run.id)
        return exc
