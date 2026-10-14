from typing import Any, Sequence, cast

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
    target: (
        ModelTarget[DataPointVersion | DataPoint]
        | ExtendedTarget[DataPointVersion | DataPoint]
    ) = ExtendedTarget(
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

    def _active_dimension_columns(self, values: Values | None) -> list[str]:
        """Extra columns for the active filters, ordered most-selective first."""
        if not values:
            return []
        dims = [key for key in ("region", "variable", "unit") if key in values]
        runchain = [key for key in ("model", "scenario") if key in values]
        run = values.get("run")
        run_filtered = isinstance(run, dict) and (
            run.get("default_only") is True
            or any(
                key in run
                for key in ("id", "id__in", "version", "version__in", "is_default")
            )
        )
        columns = dims + runchain if dims else runchain
        if run_filtered:
            columns.append("version")
        return columns

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
            columns=columns,
            limit=limit,
            offset=offset,
            join_order=(
                join_order
                if join_order is not None
                else self._active_dimension_columns(values) or None
            ),
        )

    def count(self, values: Values | None = None) -> int:
        # Use the same join structure and order as the
        # tabulation.
        columns = self._active_dimension_columns(values)
        if not columns:
            columns = self.target.pk_columns().keys()
        exc = self.select_for_values(
            values,
            columns=columns,
            join_order=columns,  # pass to avoid re-computing
        )
        count_exc = sa.select(sa.func.count()).select_from(exc.subquery())

        with self.executor.select(count_exc) as result, self.expect_one_result():
            return cast(int, result.scalar_one())

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
