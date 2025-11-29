# from .filter import TimeSeriesFilter

from toolkit import db

from ixmp4.data.iamc.variable.db import Variable
from ixmp4.data.region.db import Region
from ixmp4.data.unit.db import Unit

from .db import TimeSeries
from .exceptions import TimeSeriesNotFound, TimeSeriesNotUnique
from .filter import TimeSeriesFilter


class ItemRepository(db.r.ItemRepository[TimeSeries]):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    target = db.r.ModelTarget(TimeSeries)
    filter = db.r.Filter(TimeSeriesFilter, TimeSeries)


class PandasRepository(db.r.PandasRepository):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    filter = db.r.Filter(TimeSeriesFilter, TimeSeries)
    target = db.r.ExtendedTarget(
        TimeSeries,
        {
            "region": (TimeSeries.region, Region.name),
            "variable": (TimeSeries.variable, Variable.name),
            "unit": (TimeSeries.unit, Unit.name),
        },
    )

    def delete_orphans(self) -> int | None:
        exc = self.target.delete_statement()
        exc = exc.where(~TimeSeries.datapoints.any())

        with self.executor.delete(exc) as rowcount:
            return rowcount
