from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.repositories.base import Values
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.iamc.measurand.db import Measurand
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


class DataPointAuthRepository(AuthRepository[DataPointVersion | DataPoint]):
    def where_authorized(
        self,
        exc: sa.Select[*tuple[Any, ...]] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[*tuple[Any, ...]] | sa.Update | sa.Delete:
        ts_exc = self.select_permitted_ts_ids(auth_ctx, platform)
        if ts_exc is None:
            return exc
        return exc.where(DataPoint.time_series__id.in_(ts_exc))


class PandasRepository(DataPointAuthRepository, BasePandasRepository):
    NotFound = DataPointNotFound
    NotUnique = DataPointNotUnique

    # Declaration order controls the emitted JOIN order (more relations than
    # Postgres reorders by default); dimensions come before the run chain.
    target: ModelTarget[DataPointVersion | DataPoint] = ExtendedTarget(
        DataPoint,
        {
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
            "model": ((DataPoint.timeseries, TimeSeries.run, Run.model), Model.name),
            "scenario": (
                (DataPoint.timeseries, TimeSeries.run, Run.scenario),
                Scenario.name,
            ),
            "version": ((DataPoint.timeseries, TimeSeries.run), Run.version),
            "run__id": ((DataPoint.timeseries), TimeSeries.run__id),
        },
    )
    filter = Filter(DataPointFilter, DataPoint)
    dtypes = {"step_year": "Int64"}

    def _join_order_for(self, values: Values | None) -> Sequence[str] | None:
        """Infer optimal JOIN order for the active filters."""
        if values is None:
            return None
        dims_active = any(key in values for key in ("region", "variable", "unit"))
        run = values.get("run")
        runchain_active = any(key in values for key in ("model", "scenario")) or (
            isinstance(run, dict) and "id" in run
        )
        if dims_active or not runchain_active:
            # Declaration order already puts the dimensions first.
            return None
        return ("model", "scenario")

    def select_for_values(
        self,
        values: Values | None = None,
        columns: Sequence[str] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        *,
        join_order: Sequence[str] | None = None,
    ) -> sa.Select[Any]:
        return super().select_for_values(
            values,
            columns,
            limit,
            offset,
            join_order=(
                join_order if join_order is not None else self._join_order_for(values)
            ),
        )

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


class VersionRepository(PandasRepository):
    target = ModelTarget(DataPointVersion)
    filter = Filter(DataPointVersionFilter, DataPointVersion)
    dtypes = {"step_year": "Int64"}
