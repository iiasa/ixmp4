from typing import Annotated

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc

from .db import TimeSeries


class TimeSeriesFilter(iamc.TimeSeriesFilter, total=False):
    region: Annotated[base.RegionFilter, TimeSeries.region]
    variable: Annotated[iamc.VariableFilter, TimeSeries.variable]
    unit: Annotated[base.UnitFilter, TimeSeries.unit]
    run: Annotated[base.RunFilter, TimeSeries.run]
