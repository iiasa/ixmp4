from ixmp4.data.db import filters as base
from ixmp4.db import filters


class OptimizationUnitFilter(base.UnitFilter, metaclass=filters.FilterMeta):
    def join(self, exc, **kwawrgs):
        return exc


class OptimizationScalarFilter(
    base.OptimizationScalarFilter, metaclass=filters.FilterMeta
):
    scalar: base.OptimizationScalarFilter | None
    run: base.RunFilter | None = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )
    unit: base.UnitFilter | None = filters.Field(
        default=OptimizationUnitFilter(id=None, name=None)
    )
