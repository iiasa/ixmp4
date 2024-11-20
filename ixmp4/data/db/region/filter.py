from typing import Any

from ixmp4 import db
from ixmp4.data.db import filters as base
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.db import Session, filters, typing_column, utils

from . import Region


class BaseIamcFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    def join_datapoints(
        self, exc: db.sql.Select[tuple[Region]], session: Session | None = None
    ) -> db.sql.Select[tuple[Region]]:
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=TimeSeries.region__id == Region.id)
        return exc


class SimpleIamcRegionFilter(
    base.RegionFilter, BaseIamcFilter, metaclass=filters.FilterMeta
):
    def join(
        self, exc: db.sql.Select[tuple[Region]], session: Session | None = None
    ) -> db.sql.Select[tuple[Region]]:
        return super().join_datapoints(exc, session)


class IamcRegionFilter(base.RegionFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    variable: base.VariableFilter | None = filters.Field(None)
    unit: base.UnitFilter | None = filters.Field(None)
    run: base.RunFilter = filters.Field(
        default=base.RunFilter(id=None, version=None, is_default=True)
    )

    def join(
        self, exc: db.sql.Select[tuple[Region]], session: Session | None = None
    ) -> db.sql.Select[tuple[Region]]:
        return super().join_datapoints(exc, session)


class RegionFilter(base.RegionFilter, BaseIamcFilter, metaclass=filters.FilterMeta):
    iamc: IamcRegionFilter | filters.Boolean | None = filters.Field(None)

    def filter_iamc(
        self,
        exc: db.sql.Select[tuple[Region]],
        c: typing_column[Any],  # Any since it is unused
        v: bool | None,
        session: Session | None = None,
    ) -> db.sql.Select[tuple[Region]]:
        if v is None:
            return exc

        if v is True:
            return self.join_datapoints(exc, session)
        else:
            ids = self.join_datapoints(db.select(Region.id), session)
            exc = exc.where(~Region.id.in_(ids))
            return exc

    def join(
        self, exc: db.sql.Select[tuple[Region]], session: Session | None = None
    ) -> db.sql.Select[tuple[Region]]:
        return exc
