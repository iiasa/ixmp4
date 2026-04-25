from typing import Annotated, Any

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository

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
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    model_ids_with_timeseries = (
        sa.select(Run.model__id)
        .where(Run.id.in_(sa.select(TimeSeries.run__id).distinct()))
        .distinct()
    )

    if value is True:
        return exc.where(Model.id.in_(model_ids_with_timeseries))
    elif value is False:
        return exc.where(~Model.id.in_(model_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc.where(Model.id.in_(model_ids_with_timeseries))


class ModelFilter(base.ModelFilter, total=False):
    iamc: Annotated[IamcModelFilter | bool | None, filter_by_iamc]
