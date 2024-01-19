from ixmp4.data.db import filters as base
from ixmp4.db import filters


class OptimizationTableFilter(
    base.OptimizationTableFilter, metaclass=filters.FilterMeta
):
    table: base.OptimizationTableFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )
