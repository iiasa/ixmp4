from toolkit import db
from toolkit.exceptions import NotFound, NotUnique

from ixmp4.core.exceptions import DeletionPrevented
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries
from ixmp4.rewrite.data.iamc.variable.db import Variable
from ixmp4.rewrite.data.model.db import Model
from ixmp4.rewrite.data.region.db import Region
from ixmp4.rewrite.data.run.db import Run
from ixmp4.rewrite.data.scenario.db import Scenario
from ixmp4.rewrite.data.unit.db import Unit

from .db import DataPoint
from .filter import DataPointFilter


class MeasurandNotFound(NotFound):
    pass


class MeasurandNotUnique(NotUnique):
    pass


class MeasurandDeletionPrevented(DeletionPrevented):
    pass


class PandasRepository(db.r.PandasRepository):
    NotFound = MeasurandNotFound
    NotUnique = MeasurandNotUnique
    target = db.r.ExtendedTarget(
        DataPoint,
        {
            "model": ((DataPoint.timeseries, TimeSeries.run, Run.model), Model.name),
            "scenario": (
                (DataPoint.timeseries, TimeSeries.run, Run.scenario),
                Scenario.name,
            ),
            "version": ((DataPoint.timeseries, TimeSeries.run), Run.version),
            "region": ((DataPoint.timeseries, TimeSeries.region), Region.name),
            "variable": ((DataPoint.timeseries, TimeSeries.variable), Variable.name),
            "unit": ((DataPoint.timeseries, TimeSeries.unit), Unit.name),
        },
    )
    filter = db.r.Filter(DataPointFilter, DataPoint)
