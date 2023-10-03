from typing_extensions import Annotated

from ixmp4.db import filters, utils

from .. import Measurand, TimeSeries, Unit


class UnitFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    name: Annotated[filters.String | None, filters.Field(None)]

    _sqla_model = Unit

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        exc = exc.join(Unit, Measurand.unit)
        return exc
