from ixmp4.data.db import filters as base
from ixmp4.db import filters


class TimeSeriesFilter(base.TimeSeriesFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    run: base.RunFilter = filters.Field(default=base.RunFilter())
    region: base.RegionFilter | None
    variable: base.VariableFilter | None
    unit: base.UnitFilter | None

    def join(self, exc, **kwargs):
        return exc
