from toolkit import db

from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries
from ixmp4.rewrite.data.iamc.variable.db import Variable
from ixmp4.rewrite.data.model.db import Model
from ixmp4.rewrite.data.region.db import Region
from ixmp4.rewrite.data.run.db import Run
from ixmp4.rewrite.data.scenario.db import Scenario
from ixmp4.rewrite.data.unit.db import Unit
from ixmp4.rewrite.exceptions import DeletionPrevented, NotFound, NotUnique, registry

from .db import DataPoint
from .filter import DataPointFilter


@registry.register()
class DataPointNotFound(NotFound):
    pass


@registry.register()
class DataPointNotUnique(NotUnique):
    pass


@registry.register()
class DataPointDeletionPrevented(DeletionPrevented):
    pass


class PandasRepository(db.r.PandasRepository):
    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique
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
