from typing_extensions import Annotated

from ixmp4.db import filters

from .. import Region, TimeSeries


class RegionFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    name: Annotated[filters.String | None, filters.Field(None)]
    hierarchy: Annotated[filters.String | None, filters.Field(None)]

    _sqla_model = Region

    def join(self, exc, **kwargs):
        exc = exc.join(Region, TimeSeries.region)
        return exc
