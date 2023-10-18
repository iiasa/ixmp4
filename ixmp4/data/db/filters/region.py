from typing import ClassVar

from ixmp4.db import filters

from .. import Region, TimeSeries


class RegionFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    hierarchy: filters.String

    sqla_model: ClassVar[type] = Region

    def join(self, exc, **kwargs):
        exc = exc.join(Region, TimeSeries.region)
        return exc
