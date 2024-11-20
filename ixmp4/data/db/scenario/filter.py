from typing import Any

from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.data.db.run.model import Run
from ixmp4.db import filters, typing_column, utils

from .model import Scenario


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(
        self, exc: db.sql.Select[tuple[Scenario]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Scenario]]:
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, onclause=Run.scenario__id == Scenario.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.run__id == Run.id)

        return exc


class RunFilter(base.RunFilter, metaclass=filters.FilterMeta):
    def join(
        self, exc: db.sql.Select[tuple[Scenario]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Scenario]]:
        if not utils.is_joined(exc, Run):
            # TODO should this really be Run.model?
            exc = exc.join(Run, Run.model)
        return exc

    scenario: base.ScenarioFilter | None = filters.Field(default=None, exclude=True)


class IamcScenarioFilter(
    base.ScenarioFilter, BaseIamcFilter, metaclass=filters.FilterMeta
):
    region: base.RegionFilter | None = filters.Field(None)
    variable: base.VariableFilter | None = filters.Field(None)
    unit: base.UnitFilter | None = filters.Field(None)
    run: RunFilter = filters.Field(
        default=RunFilter(id=None, version=None, is_default=True)
    )

    def join(
        self, exc: db.sql.Select[tuple[Scenario]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Scenario]]:
        return super().join_datapoints(exc, session)


class ScenarioFilter(base.ScenarioFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: IamcScenarioFilter | filters.Boolean

    def filter_iamc(
        self,
        exc: db.sql.Select[tuple[Scenario]],
        c: typing_column[Any],  # Any since it is unused
        v: bool | None,
        session: db.Session | None = None,
    ) -> db.sql.Select[tuple[Scenario]]:
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Scenario.id), session)
            exc = exc.where(~Scenario.id.in_(ids))
            return exc

    def join(
        self, exc: db.sql.Select[tuple[Scenario]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Scenario]]:
        return exc
