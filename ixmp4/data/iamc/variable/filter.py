from typing import Annotated

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.filter import RunFilter
from ixmp4.data.versions.filter import VersionFilter

from .db import Variable


class VariableFilter(iamc.VariableFilter, total=False):
    unit: Annotated[base.UnitFilter, (Variable.timeseries, TimeSeries.unit)]
    region: Annotated[base.RegionFilter, (Variable.timeseries, TimeSeries.region)]
    run: Annotated[RunFilter, (Variable.timeseries, TimeSeries.run)]


class VariableVersionFilter(VersionFilter, iamc.VariableFilter, total=False):
    pass
