# from .filter import TimeSeriesFilter
from typing import Any

from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented

from .db import TimeSeries


class TimeSeriesNotFound(NotFound):
    pass


class TimeSeriesNotUnique(NotUnique):
    pass


class TimeSeriesDeletionPrevented(DeletionPrevented):
    pass


class ItemRepository(db.r.ItemRepository[TimeSeries]):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    target = db.r.ModelTarget(TimeSeries)
    # filter = db.r.Filter(TimeSeriesFilter, TimeSeries)


class PandasRepository(db.r.PandasRepository):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    target = db.r.ModelTarget(TimeSeries)

    # filter = db.r.Filter(TimeSeriesFilter, TimeSeries)
    def delete_orphans(self) -> Any:
        exc = self.target.delete_statement()
        exc = exc.where(~TimeSeries.datapoints.any())

        with self.executor.delete(exc) as rowcount:
            return rowcount
