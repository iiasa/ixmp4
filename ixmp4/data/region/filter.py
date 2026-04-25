from typing import Annotated, Any

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.measurand.db import Measurand
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.filter import RunFilter

from .db import Region


class IamcRegionFilter(base.RegionFilter, total=False):
    variable: Annotated[
        iamc.VariableFilter,
        (Region.timeseries, TimeSeries.measurand, Measurand.variable),
    ]
    unit: Annotated[base.UnitFilter, (Region.timeseries, TimeSeries.unit)]
    run: Annotated[RunFilter, (Region.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    region_ids_with_timeseries = sa.select(sa.distinct(TimeSeries.region__id))
    if value is True:
        return exc.where(Region.id.in_(region_ids_with_timeseries))
    elif value is False:
        return exc.where(Region.id.notin_(region_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc.where(Region.id.in_(region_ids_with_timeseries))


class RegionFilter(base.RegionFilter, total=False):
    iamc: Annotated[IamcRegionFilter | bool | None, filter_by_iamc]
