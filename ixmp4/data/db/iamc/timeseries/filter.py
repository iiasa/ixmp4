from ixmp4.data.db import filters as base
from ixmp4.db import Session, filters, sql

from .model import TimeSeries


class TimeSeriesFilter(base.TimeSeriesFilter, metaclass=filters.FilterMeta):
    run: base.RunFilter = filters.Field(default=base.RunFilter())
    region: base.RegionFilter | None = filters.Field(None)
    variable: base.VariableFilter | None = filters.Field(None)
    unit: base.UnitFilter | None = filters.Field(None)

    def join(
        self, exc: sql.Select[tuple[TimeSeries]], session: Session | None = None
    ) -> sql.Select[tuple[TimeSeries]]:
        return exc
