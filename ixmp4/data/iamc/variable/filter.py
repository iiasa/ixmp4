from typing import Annotated, Any

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.measurand.db import Measurand
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.filter import RunFilter
from ixmp4.data.versions.filter import VersionFilter

from .db import Variable


def filter_by_run(
    exc: sa.Select[Any],
    value: dict[str, Any] | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any]:
    # None means no run filter; dict case is handled entirely by path traversal.
    return exc


class VariableFilter(iamc.VariableFilter, total=False):
    unit: Annotated[base.UnitFilter, (Variable.timeseries, TimeSeries.unit)]
    region: Annotated[base.RegionFilter, (Variable.timeseries, TimeSeries.region)]
    run: Annotated[
        RunFilter | None,
        (Variable.measurands, Measurand.timeseries, TimeSeries.run),
        filter_by_run,
    ]


class VariableVersionFilter(VersionFilter, iamc.VariableFilter, total=False):
    pass
