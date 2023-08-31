from ixmp4.data.db import filters as base
from ixmp4.db import filters


class OptimizationIndexSetFilter(
    base.OptimizationIndexSetFilter, metaclass=filters.FilterMeta
):
    indexset: base.OptimizationIndexSetFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None)
    )

    def join(self, exc, session=None):
        return exc
