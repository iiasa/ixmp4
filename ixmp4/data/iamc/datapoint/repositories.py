from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.repository.base import Values

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.iamc.variable.db import Variable
from ixmp4.data.model.db import Model
from ixmp4.data.region.db import Region
from ixmp4.data.run.db import Run
from ixmp4.data.scenario.db import Scenario
from ixmp4.data.unit.db import Unit

from .db import DataPoint, DataPointVersion
from .exceptions import DataPointNotFound, DataPointNotUnique
from .filter import DataPointFilter, DataPointVersionFilter


class DataPointAuthRepository(AuthRepository[DataPoint]):
    def select_permitted_ts_ids(
        self, auth_ctx: AuthorizationContext, platform: PlatformProtocol
    ) -> sa.Select[tuple[int]]:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return (
            sa.select(TimeSeries)
            .where(TimeSeries.run__id.in_(run_exc))
            .with_only_columns(TimeSeries.id)
        )

    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        ts_exc = self.select_permitted_ts_ids(auth_ctx, platform)
        return exc.where(DataPoint.time_series__id.in_(ts_exc))


class PandasRepository(DataPointAuthRepository, db.r.PandasRepository):
    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique
    target = db.r.ExtendedTarget(
        DataPoint,
        {
            "model": ((DataPoint.timeseries, TimeSeries.run, Run.model), Model.name),
            "scenario": (
                (DataPoint.timeseries, TimeSeries.run, Run.scenario),
                Scenario.name,
            ),
            "version": ((DataPoint.timeseries, TimeSeries.run), Run.version),
            "region": ((DataPoint.timeseries, TimeSeries.region), Region.name),
            "variable": ((DataPoint.timeseries, TimeSeries.variable), Variable.name),
            "unit": ((DataPoint.timeseries, TimeSeries.unit), Unit.name),
            "run__id": ((DataPoint.timeseries), TimeSeries.run__id),
        },
    )
    filter = db.r.Filter(DataPointFilter, DataPoint)
    dtypes = {"step_year": "Int64"}

    def tabulate(
        self,
        values: Values | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> pd.DataFrame:
        df = super().tabulate(values, columns, limit, offset)

        # drop empty step columns
        cols_to_check = ["step_year", "step_category", "step_datetime"]
        cols_to_drop = [
            col for col in cols_to_check if col in df.columns and df[col].isna().all()
        ]
        return df.drop(columns=cols_to_drop)


class VersionRepository(db.r.PandasRepository):
    target = db.r.ModelTarget(DataPointVersion)
    filter = db.r.Filter(DataPointVersionFilter, DataPointVersion)
