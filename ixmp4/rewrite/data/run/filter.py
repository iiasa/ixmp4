from typing import Annotated, Any

import sqlalchemy as sa
from toolkit import db

from ixmp4.rewrite.data import filters as base

from .db import Run


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: db.r.BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True:
        return exc.where(Run.timeseries.has())
    elif value is False:
        return exc.where(~Run.timeseries.has())
    elif value is None:
        return exc
    else:
        return exc.where(Run.timeseries.has())


class RunFilter(base.RunFilter, total=False):
    model: Annotated[base.ModelFilter, Run.model]
    scenario: Annotated[base.ScenarioFilter, Run.scenario]
