from typing import Annotated

from ixmp4.rewrite.data import filters as base
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries

from .db import Variable


class VariableFilter(base.VariableFilter, total=False):
    variable: Annotated[base.VariableFilter, (Variable.timeseries, TimeSeries.variable)]
    region: Annotated[base.RegionFilter, (Variable.timeseries, TimeSeries.region)]
    run: Annotated[base.RunFilter, (Variable.timeseries, TimeSeries.run)]
