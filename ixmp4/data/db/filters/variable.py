from typing import ClassVar

from ixmp4.db import Session, filters, utils

from .. import Measurand, TimeSeries, Variable
from ..base import SelectType


class VariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Variable

    def join(self, exc: SelectType, session: Session | None = None) -> SelectType:
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        exc = exc.join(Variable, Measurand.variable)
        return exc
