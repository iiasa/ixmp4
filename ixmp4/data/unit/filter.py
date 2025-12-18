from typing import Annotated, Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.filter import RunFilter

from .db import Unit


class IamcUnitFilter(base.UnitFilter, total=False):
    variable: Annotated[iamc.VariableFilter, (Unit.timeseries, TimeSeries.variable)]
    region: Annotated[base.RegionFilter, (Unit.timeseries, TimeSeries.region)]
    run: Annotated[RunFilter, (Unit.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: db.r.BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True:
        return exc.where(Unit.timeseries.any())
    elif value is False:
        return exc.where(~Unit.timeseries.any())
    elif value is None:
        return exc
    else:
        return exc.where(Unit.timeseries.any())


class UnitFilter(base.UnitFilter, total=False):
    iamc: Annotated[IamcUnitFilter | bool | None, filter_by_iamc]
