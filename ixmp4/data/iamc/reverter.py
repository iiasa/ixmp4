from typing import Any

import sqlalchemy as sa
from toolkit import db

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
    target = db.r.ModelTarget(DataPoint)
    version_target = db.r.ModelTarget(DataPointVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(DataPointVersion).where(
            DataPointVersion.timeseries.has(TimeSeriesVersion.run__id == run__id)
        )


class MeasurandReverterRepository(ReverterRepository[[int]]):
    target = db.r.ModelTarget(Measurand)
    version_target = db.r.ModelTarget(MeasurandVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(MeasurandVersion).where(
            MeasurandVersion.timeseries.has(TimeSeriesVersion.run__id == run__id)
        )


class TimeSeriesReverterRepository(ReverterRepository[[int]]):
    target = db.r.ModelTarget(TimeSeries)
    version_target = db.r.ModelTarget(TimeSeriesVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TimeSeriesVersion).where(TimeSeriesVersion.run__id == run__id)


run_reverter = Reverter(
    targets=[
        DataPointReverterRepository,
        MeasurandReverterRepository,
        TimeSeriesReverterRepository,
    ]
)
