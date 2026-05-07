from typing import Annotated

from ixmp4.data.filters import iamc
from ixmp4.data.versions.filter import VersionFilter

from .db import Measurand, MeasurandVersion


class MeasurandFilter(iamc.MeasurandFilter, total=False):
    timeseries: Annotated[iamc.TimeSeriesFilter, (Measurand.timeseries)]


class MeasurandVersionFilter(iamc.MeasurandFilter, VersionFilter, total=False):
    timeseries: Annotated[iamc.TimeSeriesFilter, (MeasurandVersion.timeseries)]
