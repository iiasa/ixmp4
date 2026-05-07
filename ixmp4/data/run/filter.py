from typing import Annotated, Any

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository
from typing_extensions import TypedDict

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.measurand.db import Measurand
from ixmp4.data.iamc.timeseries.db import TimeSeries

from .db import Run


class IamcRunFilter(TypedDict, total=False):
    variable: Annotated[
        iamc.VariableFilter,
        (Run.timeseries, TimeSeries.measurand, Measurand.variable),
    ]
    unit: Annotated[base.UnitFilter, (Run.timeseries, TimeSeries.unit)]
    region: Annotated[base.RegionFilter, (Run.timeseries, TimeSeries.region)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    run_ids_with_timeseries = sa.select(sa.distinct(TimeSeries.run__id))
    if value is True or value == {}:
        return exc.where(Run.id.in_(run_ids_with_timeseries))
    elif value is False:
        return exc.where(Run.id.notin_(run_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc


class RunFilter(base.RunFilter, total=False):
    model: Annotated[base.ModelFilter, Run.model]
    scenario: Annotated[base.ScenarioFilter, Run.scenario]
    iamc: Annotated[IamcRunFilter | bool | None, filter_by_iamc]
