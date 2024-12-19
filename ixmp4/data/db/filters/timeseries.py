from typing import ClassVar

from ixmp4.db import Session, filters, utils

from .. import Run, TimeSeries
from ..base import SelectType


class TimeSeriesFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)

    sqla_model: ClassVar[type] = TimeSeries

    def join(self, exc: SelectType, session: Session | None = None) -> SelectType:
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)
        return exc
