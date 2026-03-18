from typing import Any

import sqlalchemy as sa
from toolkit.db.target import ModelTarget

from ixmp4.data.iamc.datapoint.db import (
    DataPoint,
    DataPointVersion,
)
from ixmp4.data.iamc.measurand.db import (
    Measurand,
    MeasurandVersion,
)
from ixmp4.data.iamc.timeseries.db import (
    TimeSeries,
    TimeSeriesVersion,
)
from ixmp4.data.versions.reverter import Reverter, ReverterRepository


class DataPointReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(DataPoint)
    version_target = ModelTarget(DataPointVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(DataPointVersion).where(
            DataPointVersion.timeseries.has(TimeSeriesVersion.run__id == run__id)
        )


class MeasurandReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Measurand)
    version_target = ModelTarget(MeasurandVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(MeasurandVersion).where(
            MeasurandVersion.timeseries.has(TimeSeriesVersion.run__id == run__id)
        )


class TimeSeriesReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(TimeSeries)
    version_target = ModelTarget(TimeSeriesVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TimeSeriesVersion).where(TimeSeriesVersion.run__id == run__id)


run_reverter = Reverter(
    targets=[
        DataPointReverterRepository,
        MeasurandReverterRepository,
        TimeSeriesReverterRepository,
    ]
)
