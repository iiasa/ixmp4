from typing import ClassVar

from ixmp4.db import Session, filters, sql

from .. import Region, TimeSeries


class RegionFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)
    hierarchy: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Region

    def join(self, exc: sql.Select, session: Session | None = None) -> sql.Select:
        exc = exc.join(Region, TimeSeries.region)
        return exc
