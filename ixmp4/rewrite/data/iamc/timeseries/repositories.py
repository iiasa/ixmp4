# from .filter import TimeSeriesFilter

from toolkit import db

from ixmp4.rewrite.data.iamc.variable.db import Variable
from ixmp4.rewrite.data.region.db import Region
from ixmp4.rewrite.data.unit.db import Unit
from ixmp4.rewrite.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import TimeSeries


@registry.register()
class TimeSeriesNotFound(NotFound):
    pass


@registry.register()
class TimeSeriesNotUnique(NotUnique):
    pass


@registry.register()
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
    target = db.r.ExtendedTarget(
        TimeSeries,
        {
            "region": (TimeSeries.region, Region.name),
            "variable": (TimeSeries.variable, Variable.name),
            "unit": (TimeSeries.unit, Unit.name),
        },
    )

    # filter = db.r.Filter(TimeSeriesFilter, TimeSeries)
    def delete_orphans(self) -> int | None:
        exc = self.target.delete_statement()
        exc = exc.where(~TimeSeries.datapoints.any())

        with self.executor.delete(exc) as rowcount:
            return rowcount
