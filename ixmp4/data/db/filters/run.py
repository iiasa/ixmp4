from typing import ClassVar

from ixmp4.db import filters, utils

from .. import Run, TimeSeries
from .model import ModelFilter
from .scenario import ScenarioFilter


class RunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    version: filters.Integer | None = filters.Field(None)
    default_only: filters.Boolean | None = filters.Field(True)
    is_default: filters.Boolean | None = filters.Field(None)
    model: ModelFilter = filters.Field(None)
    scenario: ScenarioFilter = filters.Field(None)

    sqla_model: ClassVar[type] = Run

    def filter_default_only(self, exc, c, v, **kwargs):
        if v:
            return exc.where(Run.is_default)
        else:
            return exc

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        return exc
