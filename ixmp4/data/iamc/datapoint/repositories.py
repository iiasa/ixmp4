from typing import Any, Sequence

import pandas as pd
import sqlalchemy as sa
from toolkit.auth.context import AuthorizationContext, PlatformProtocol
from toolkit.db.filter import Filter
from toolkit.db.repositories import PandasRepository as BasePandasRepository
from toolkit.db.repositories.base import Values
from toolkit.db.target import ExtendedTarget, ModelTarget

from ixmp4.base_exceptions import ProgrammingError
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
from .type import Type


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


class PandasRepository(DataPointAuthRepository, BasePandasRepository):
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

    def describe(self, values: Values | None = None) -> dict[str, Any]:
        delimiter = "\x1f"
        dialect = self.executor.engine.dialect.name

        filtered = (
            self.select_for_values(
                values=values,
                columns=(
                    "id",
                    "type",
                    "step_year",
                    "step_datetime",
                    "step_category",
                    "value",
                ),
            )
            .distinct()
            .cte("filtered_datapoints")
        )

        ordered_values = sa.select(
            filtered.c.value,
            (
                sa.func.row_number().over(order_by=filtered.c.value) - sa.literal(1)
            ).label("idx"),
        ).cte("ordered_values")

        count = sa.select(
            sa.select(sa.func.count())
            .select_from(filtered)
            .scalar_subquery()
            .label("count"),
        ).cte("count")

        def percentile_expr(p: float) -> sa.ColumnElement[float | None]:
            rank = (sa.cast(count.c.count, sa.Float) - sa.literal(1.0)) * sa.literal(p)
            lower_idx = sa.cast(sa.func.floor(rank), sa.Integer)
            upper_idx = sa.cast(sa.func.ceil(rank), sa.Integer)
            fraction = rank - sa.cast(lower_idx, sa.Float)

            lower_val = (
                sa.select(ordered_values.c.value)
                .where(ordered_values.c.idx == lower_idx)
                .scalar_subquery()
            )
            upper_val = (
                sa.select(ordered_values.c.value)
                .where(ordered_values.c.idx == upper_idx)
                .scalar_subquery()
            )

            interp = sa.cast(lower_val, sa.Float) + fraction * (
                sa.cast(upper_val, sa.Float) - sa.cast(lower_val, sa.Float)
            )

            return sa.case(
                (count.c.count == 0, sa.null()),
                (lower_idx == upper_idx, sa.cast(lower_val, sa.Float)),
                else_=interp,
            )

        categories_sorted = (
            sa.select(filtered.c.step_category.label("item"))
            .where(filtered.c.type == Type.CATEGORICAL)
            .where(filtered.c.step_category.is_not(None))
            .distinct()
            .order_by(filtered.c.step_category)
            .cte("categories_sorted")
        )
        types_sorted = (
            sa.select(sa.cast(filtered.c.type, sa.String).label("item"))
            .where(filtered.c.type.is_not(None))
            .distinct()
            .order_by(sa.cast(filtered.c.type, sa.String))
            .cte("types_sorted")
        )

        if dialect == "sqlite":
            categories_joined = sa.select(
                sa.func.coalesce(
                    sa.func.group_concat(categories_sorted.c.item, delimiter), ""
                )
            ).scalar_subquery()
            types_joined = sa.select(
                sa.func.coalesce(
                    sa.func.group_concat(types_sorted.c.item, delimiter), ""
                )
            ).scalar_subquery()
        elif dialect == "postgresql":
            categories_joined = sa.select(
                sa.func.coalesce(
                    sa.func.string_agg(categories_sorted.c.item, delimiter), ""
                )
            ).scalar_subquery()
            types_joined = sa.select(
                sa.func.coalesce(sa.func.string_agg(types_sorted.c.item, delimiter), "")
            ).scalar_subquery()
        else:
            raise ProgrammingError("Unsupported database dialect: " + dialect)

        describe_exc = sa.select(
            count.c.count,
            sa.select(sa.func.min(filtered.c.value)).scalar_subquery().label("min"),
            percentile_expr(0.25).label("p25"),
            percentile_expr(0.5).label("median"),
            percentile_expr(0.75).label("p75"),
            sa.select(sa.func.max(filtered.c.value)).scalar_subquery().label("max"),
            sa.select(sa.func.min(filtered.c.step_year))
            .scalar_subquery()
            .label("first_year"),
            sa.select(sa.func.max(filtered.c.step_year))
            .scalar_subquery()
            .label("last_year"),
            sa.select(sa.func.min(filtered.c.step_datetime))
            .scalar_subquery()
            .label("first_datetime"),
            sa.select(sa.func.max(filtered.c.step_datetime))
            .scalar_subquery()
            .label("last_datetime"),
            categories_joined.label("categories"),
            types_joined.label("types"),
        ).select_from(count)

        with self.executor.select(describe_exc) as result:
            row = result.mappings().one()

        aggregations = dict(row)
        raw_types = [item for item in str(row["types"]).split(delimiter) if item]
        aggregations["types"] = [type_ for type_ in Type if type_.value in raw_types]
        aggregations["categories"] = [
            item for item in str(row["categories"]).split(delimiter) if item
        ]
        return aggregations


class VersionRepository(PandasRepository):
    target = ModelTarget(DataPointVersion)
    filter = Filter(DataPointVersionFilter, DataPointVersion)
    dtypes = {"step_year": "Int64"}
