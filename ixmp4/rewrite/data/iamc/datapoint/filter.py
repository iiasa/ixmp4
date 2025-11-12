from typing import Annotated

from ixmp4.rewrite.data import filters as base
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries
from ixmp4.rewrite.data.run.db import Run

from .db import DataPoint


class DataPointFilter(base.DataPointFilter, total=False):
    region: Annotated[base.RegionFilter, (DataPoint.timeseries, TimeSeries.region)]
    variable: Annotated[
        base.VariableFilter, (DataPoint.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (DataPoint.timeseries, TimeSeries.unit)]
    run: Annotated[base.RunFilter, (DataPoint.timeseries, TimeSeries.run)]
    model: Annotated[
        base.ModelFilter, (DataPoint.timeseries, TimeSeries.run, Run.model)
    ]
    scenario: Annotated[
        base.ScenarioFilter, (DataPoint.timeseries, TimeSeries.run, Run.scenario)
    ]
