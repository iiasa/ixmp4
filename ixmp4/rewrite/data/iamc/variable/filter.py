from typing import Annotated

from ixmp4.rewrite.data import filters as base
from ixmp4.rewrite.data.filters import iamc as iamc
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries

from .db import Variable


class VariableFilter(iamc.VariableFilter, total=False):
    variable: Annotated[iamc.VariableFilter, (Variable.timeseries, TimeSeries.variable)]
    region: Annotated[base.RegionFilter, (Variable.timeseries, TimeSeries.region)]
    run: Annotated[base.RunFilter, (Variable.timeseries, TimeSeries.run)]
