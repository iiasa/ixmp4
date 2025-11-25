from typing import Annotated, Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.db import Run

from .db import Model


class IamcModelFilter(base.ModelFilter, total=False):
    variable: Annotated[
        iamc.VariableFilter, (Model.runs, Run.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (Model.runs, Run.timeseries, TimeSeries.unit)]
    region: Annotated[
        base.RegionFilter, (Model.runs, Run.timeseries, TimeSeries.region)
    ]
    run: Annotated[base.RunFilter, (Model.runs, Run.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: db.r.BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True:
        return exc.where(Model.runs.has(Run.timeseries.has()))
    elif value is False:
        return exc.where(~Model.runs.has(Run.timeseries.has()))
    elif value is None:
        return exc
    else:
        return exc.where(Model.runs.has(Run.timeseries.has()))


class ModelFilter(base.ModelFilter, total=False):
    iamc: Annotated[IamcModelFilter | bool | None, filter_by_iamc]
