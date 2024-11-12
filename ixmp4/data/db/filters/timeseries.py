from typing import ClassVar

from ixmp4.db import Session, filters, sql, utils

from .. import Run, TimeSeries


class TimeSeriesFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)

    sqla_model: ClassVar[type] = TimeSeries

    def join(self, exc: sql.Select, session: Session | None = None) -> sql.Select:
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)
        return exc
