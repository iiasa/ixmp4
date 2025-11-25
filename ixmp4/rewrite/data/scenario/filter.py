from typing import Annotated, Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.data import filters as base
from ixmp4.rewrite.data.filters import iamc as iamc
from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries
from ixmp4.rewrite.data.run.db import Run

from .db import Scenario


class IamcScenarioFilter(base.ScenarioFilter, total=False):
    variable: Annotated[
        iamc.VariableFilter, (Scenario.runs, Run.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (Scenario.runs, Run.timeseries, TimeSeries.unit)]
    region: Annotated[
        base.RegionFilter, (Scenario.runs, Run.timeseries, TimeSeries.region)
    ]
    run: Annotated[base.RunFilter, (Scenario.runs, Run.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: db.r.BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True:
        return exc.where(Scenario.runs.any(Run.timeseries.any()))
    elif value is False:
        return exc.where(~Scenario.runs.any(Run.timeseries.any()))
    elif value is None:
        return exc
    else:
        return exc.where(Scenario.runs.any(Run.timeseries.any()))


class ScenarioFilter(base.ScenarioFilter, total=False):
    iamc: Annotated[IamcScenarioFilter | bool | None, filter_by_iamc]
