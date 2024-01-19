from ixmp4.data.db import filters as base
from ixmp4.db import filters


class OptimizationColumnFilter(
    base.OptimizationColumnFilter, metaclass=filters.FilterMeta
):
    table: base.OptimizationColumnFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )
