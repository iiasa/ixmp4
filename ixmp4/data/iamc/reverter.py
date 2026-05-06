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
from ixmp4.data.iamc.variable.db import Variable, VariableVersion
from ixmp4.data.versions.reverter import Reverter, ReverterRepository


class DataPointReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(DataPoint)
    version_target = ModelTarget(DataPointVersion)
    dtypes = {"step_year": "Int64"}

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(DataPointVersion).where(
            DataPointVersion.timeseries.has(TimeSeriesVersion.run__id == run__id)
        )


class IamcVariableReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Variable)
    version_target = ModelTarget(VariableVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(VariableVersion).where(
            sa.exists(
                sa.select(sa.literal(1))
                .select_from(TimeSeriesVersion)
                .join(
                    MeasurandVersion,
                    MeasurandVersion.id == TimeSeriesVersion.measurand__id,
                )
                .where(
                    MeasurandVersion.variable__id == VariableVersion.id,
                    TimeSeriesVersion.run__id == run__id,
                )
            )
        )


class MeasurandReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(Measurand)
    version_target = ModelTarget(MeasurandVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        # Use a direct EXISTS join (no join_valid_versions) so that measurands whose
        # timeseries have been deleted are still found and can be re-inserted by the
        # constructive revert phase.
        return sa.select(MeasurandVersion).where(
            sa.exists(
                sa.select(TimeSeriesVersion.id).where(
                    TimeSeriesVersion.measurand__id == MeasurandVersion.id,
                    TimeSeriesVersion.run__id == run__id,
                )
            )
        )


class TimeSeriesReverterRepository(ReverterRepository[[int]]):
    target = ModelTarget(TimeSeries)
    version_target = ModelTarget(TimeSeriesVersion)

    def select_versions(self, run__id: int) -> sa.Select[Any]:
        return sa.select(TimeSeriesVersion).where(TimeSeriesVersion.run__id == run__id)


run_reverter = Reverter(
    targets=[
        DataPointReverterRepository,
        IamcVariableReverterRepository,
        MeasurandReverterRepository,
        TimeSeriesReverterRepository,
    ]
)
