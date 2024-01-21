from typing import ClassVar

from ixmp4.db import filters, utils

from .. import Run, TimeSeries


class TimeSeriesFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id

    sqla_model: ClassVar[type] = TimeSeries

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)
        return exc
