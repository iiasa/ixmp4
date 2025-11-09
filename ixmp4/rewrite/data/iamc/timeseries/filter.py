from typing import Annotated

from ixmp4.rewrite.data import filters as base

from .db import TimeSeries


class TimeSeriesFilter(base.TimeSeriesFilter, total=False):
    region: Annotated[base.RegionFilter, TimeSeries.region]
    variable: Annotated[base.VariableFilter, TimeSeries.variable]
    unit: Annotated[base.UnitFilter, TimeSeries.unit]
    run: Annotated[base.RunFilter, TimeSeries.run]
