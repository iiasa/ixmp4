# from .filter import TimeSeriesFilter


from typing import Any, Sequence

import sqlalchemy as sa
from toolkit import db
from toolkit.auth.context import AuthorizationContext, PlatformProtocol

from ixmp4.data.base.repository import AuthRepository
from ixmp4.data.iamc.variable.db import Variable
from ixmp4.data.model.db import Model
from ixmp4.data.region.db import Region
from ixmp4.data.run.db import Run
from ixmp4.data.unit.db import Unit

from .db import TimeSeries, TimeSeriesVersion
from .exceptions import TimeSeriesNotFound, TimeSeriesNotUnique
from .filter import TimeSeriesFilter, TimeSeriesVersionFilter


class TimeSeriesAuthRepository(AuthRepository[TimeSeries]):
    def where_authorized(
        self,
        exc: sa.Select[Any] | sa.Update | sa.Delete,
        auth_ctx: AuthorizationContext,
        platform: PlatformProtocol,
    ) -> sa.Select[Any] | sa.Update | sa.Delete:
        run_exc = self.select_permitted_run_ids(auth_ctx, platform)
        return exc.where(TimeSeries.run__id.in_(run_exc))

    def list_model_names(self, ts_ids: Sequence[int]) -> Sequence[str]:
        exc = (
            sa.select(Model.name)
            .distinct()
            .select_from(TimeSeries)
            .join(TimeSeries.run)
            .join(Run.model)
            .where(TimeSeries.id.in_(ts_ids))
        )

        with self.executor.select(exc) as result:
            return result.scalars().all()


class ItemRepository(TimeSeriesAuthRepository, db.r.ItemRepository[TimeSeries]):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    target = db.r.ModelTarget(TimeSeries)
    filter = db.r.Filter(TimeSeriesFilter, TimeSeries)


class PandasRepository(TimeSeriesAuthRepository, db.r.PandasRepository):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    filter = db.r.Filter(TimeSeriesFilter, TimeSeries)
    target = db.r.ExtendedTarget(
        TimeSeries,
        {
            "region": (TimeSeries.region, Region.name),
            "variable": (TimeSeries.variable, Variable.name),
            "unit": (TimeSeries.unit, Unit.name),
        },
    )

    def delete_orphans(self) -> int | None:
        exc = self.target.delete_statement()
        exc = exc.where(~TimeSeries.datapoints.any())

        with self.executor.delete(exc) as rowcount:
            return rowcount


class VersionRepository(db.r.PandasRepository):
    NotFound = TimeSeriesNotFound
    NotUnique = TimeSeriesNotUnique
    filter = db.r.Filter(TimeSeriesVersionFilter, TimeSeriesVersion)
    target = db.r.ModelTarget(TimeSeriesVersion)
