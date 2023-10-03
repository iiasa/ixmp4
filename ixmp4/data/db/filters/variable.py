from typing_extensions import Annotated

from ixmp4.db import filters, utils

from .. import Measurand, TimeSeries, Variable


class VariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    name: Annotated[filters.String | None, filters.Field(None)]

    _sqla_model = Variable

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        exc = exc.join(Variable, Measurand.variable)
        return exc
