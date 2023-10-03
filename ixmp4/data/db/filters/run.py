from typing_extensions import Annotated

from ixmp4.db import filters, utils

from .. import Run, TimeSeries
from .model import ModelFilter
from .scenario import ScenarioFilter


class RunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: Annotated[filters.Id | None, filters.Field(None)]
    version: Annotated[filters.Integer | None, filters.Field(None)]
    default_only: Annotated[filters.Boolean, filters.Field(True)]
    is_default: Annotated[filters.Boolean, filters.Field(False)]
    model: Annotated[ModelFilter, filters.Field(None)]
    scenario: Annotated[ScenarioFilter, filters.Field(None)]

    _sqla_model = Run

    def filter_default_only(self, exc, c, v, **kwargs):
        if v:
            return exc.where(Run.is_default)
        else:
            return exc

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        return exc
