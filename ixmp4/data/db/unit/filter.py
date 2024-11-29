from typing import Any

from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.db import filters, typing_column, utils

from . import Unit


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(
        self, exc: db.sql.Select[tuple[Unit]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Unit]]:
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, Measurand.unit__id == Unit.id)

        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(
                TimeSeries, onclause=TimeSeries.measurand__id == Measurand.id
            )
        return exc


class SimpleIamcUnitFilter(
    base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: db.sql.Select[tuple[Unit]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Unit]]:
        return super().join_datapoints(exc, session)


class IamcUnitFilter(base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    variable: base.VariableFilter | None = filters.Field(None)
    region: base.RegionFilter | None = filters.Field(None)
    run: base.RunFilter = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )

    def join(
        self, exc: db.sql.Select[tuple[Unit]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Unit]]:
        return super().join_datapoints(exc, session)


class UnitFilter(base.UnitFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: IamcUnitFilter | filters.Boolean

    def filter_iamc(
        self,
        exc: db.sql.Select[tuple[Unit]],
        c: typing_column[Any],  # Any since it is unused
        v: bool | None,
        session: db.Session | None = None,
    ) -> db.sql.Select[tuple[Unit]]:
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Unit.id), session)
            exc = exc.where(~Unit.id.in_(ids))
            return exc

    def join(
        self, exc: db.sql.Select[tuple[Unit]], session: db.Session | None = None
    ) -> db.sql.Select[tuple[Unit]]:
        return exc
