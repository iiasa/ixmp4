from typing import Annotated, Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.data import filters as base
from ixmp4.rewrite.data.filters import iamc as iamc
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries

from .db import Region


class IamcRegionFilter(base.RegionFilter, total=False):
    variable: Annotated[iamc.VariableFilter, (Region.timeseries, TimeSeries.variable)]
    unit: Annotated[base.UnitFilter, (Region.timeseries, TimeSeries.unit)]
    run: Annotated[base.RunFilter, (Region.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: db.r.BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True:
        return exc.where(Region.timeseries.any())
    elif value is False:
        return exc.where(~Region.timeseries.any())
    elif value is None:
        return exc
    else:
        return exc.where(Region.timeseries.any())


class RegionFilter(base.RegionFilter, total=False):
    iamc: Annotated[IamcRegionFilter | bool | None, filter_by_iamc]
