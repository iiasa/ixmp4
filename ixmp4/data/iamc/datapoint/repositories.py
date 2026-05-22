from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.repositories.base import Values
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.iamc.measurand.db import Measurand, MeasurandVersion
from ixmp4.data.iamc.timeseries.db import TimeSeries, TimeSeriesVersion
from ixmp4.data.iamc.variable.db import Variable, VariableVersion
from ixmp4.data.model.db import Model, ModelVersion
from ixmp4.data.region.db import Region, RegionVersion
from ixmp4.data.run.db import Run, RunVersion
from ixmp4.data.scenario.db import Scenario, ScenarioVersion
from ixmp4.data.unit.db import Unit, UnitVersion

from .db import DataPoint, DataPointVersion
from .exceptions import DataPointNotFound, DataPointNotUnique
from .filter import DataPointFilter, DataPointVersionFilter


class DataPointPandasRepository(BasePandasRepository):
    step_cols = ["step_year", "step_category", "step_datetime"]

    def tabulate(
        self,
        values: Values | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        drop_empty_step_columns: bool = False,
    ) -> pd.DataFrame:
        df = super().tabulate(values, columns, limit, offset)
        # drop empty step columns
        cols_to_drop = [
            col for col in self.step_cols if col in df.columns and df[col].isna().all()
        ]
        return df.drop(columns=cols_to_drop)


class DataPointAuthRepository(AuthRepository[DataPointVersion | DataPoint]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        ts_exc = self.select_permitted_ts_ids(auth_ctx, platform)
        if ts_exc is None:
            return exc
        return exc.where(DataPoint.time_series__id.in_(ts_exc))


class DataPointVersionAuthRepository(AuthRepository[DataPointVersion]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        ts_exc = self.select_permitted_ts_ids(auth_ctx, platform)
        if ts_exc is None:
            return exc
        return exc.where(DataPointVersion.time_series__id.in_(ts_exc))


class PandasRepository(DataPointAuthRepository, DataPointPandasRepository):
    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique
    target: ModelTarget[DataPointVersion | DataPoint] = ExtendedTarget(
        DataPoint,
        {
            "model": ((DataPoint.timeseries, TimeSeries.run, Run.model), Model.name),
            "scenario": (
                (DataPoint.timeseries, TimeSeries.run, Run.scenario),
                Scenario.name,
            ),
            "version": ((DataPoint.timeseries, TimeSeries.run), Run.version),
            "region": ((DataPoint.timeseries, TimeSeries.region), Region.name),
            # Route both variable and unit through TimeSeries.measurand so SQL
            # uses a single measurand join instead of duplicated aliased joins.
            "variable": (
                (DataPoint.timeseries, TimeSeries.measurand, Measurand.variable),
                Variable.name,
            ),
            "unit": (
                (DataPoint.timeseries, TimeSeries.measurand, Measurand.unit),
                Unit.name,
            ),
            "run__id": ((DataPoint.timeseries), TimeSeries.run__id),
        },
    )
    filter = Filter(DataPointFilter, DataPoint)
    dtypes = {"step_year": "Int64"}


class VersionRepository(DataPointVersionAuthRepository, DataPointPandasRepository):
    filter = Filter(DataPointVersionFilter, DataPointVersion)
    dtypes = {"step_year": "Int64"}
    target: ModelTarget[DataPointVersion] = ExtendedTarget(
        DataPointVersion,
        {
            "model": (
                (DataPointVersion.timeseries, TimeSeriesVersion.run, RunVersion.model),
                ModelVersion.name,
            ),
            "scenario": (
                (
                    DataPointVersion.timeseries,
                    TimeSeriesVersion.run,
                    RunVersion.scenario,
                ),
                ScenarioVersion.name,
            ),
            "version": (
                (DataPointVersion.timeseries, TimeSeriesVersion.run),
                RunVersion.version,
            ),
            "region": (
                (DataPointVersion.timeseries, TimeSeriesVersion.region),
                RegionVersion.name,
            ),
            # Route both variable and unit through TimeSeries.measurand so SQL
            # uses a single measurand join instead of duplicated aliased joins.
            "variable": (
                (
                    DataPointVersion.timeseries,
                    TimeSeriesVersion.measurand,
                    MeasurandVersion.variable,
                ),
                VariableVersion.name,
            ),
            "unit": (
                (
                    DataPointVersion.timeseries,
                    TimeSeriesVersion.measurand,
                    MeasurandVersion.unit,
                ),
                UnitVersion.name,
            ),
            "run__id": ((DataPointVersion.timeseries), TimeSeriesVersion.run__id),
        },
    )
