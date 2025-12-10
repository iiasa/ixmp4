from typing import Annotated

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.db import Run
from ixmp4.data.versions.filter import VersionFilter

from .db import DataPoint, DataPointVersion


class DataPointFilter(iamc.DataPointFilter, total=False):
    region: Annotated[base.RegionFilter, (DataPoint.timeseries, TimeSeries.region)]
    variable: Annotated[
        iamc.VariableFilter, (DataPoint.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (DataPoint.timeseries, TimeSeries.unit)]
    run: Annotated[base.RunFilter, (DataPoint.timeseries, TimeSeries.run)]
    model: Annotated[
        base.ModelFilter, (DataPoint.timeseries, TimeSeries.run, Run.model)
    ]
    scenario: Annotated[
        base.ScenarioFilter, (DataPoint.timeseries, TimeSeries.run, Run.scenario)
    ]


class DataPointVersionFilter(iamc.DataPointFilter, VersionFilter, total=False):
    timeseries: Annotated[iamc.TimeSeriesFilter, (DataPointVersion.timeseries)]
