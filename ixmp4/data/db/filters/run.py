from ixmp4.db import filters, utils

from .. import Run, TimeSeries
from .model import ModelFilter
from .scenario import ScenarioFilter


class RunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    version: filters.Integer
    default_only: filters.Boolean = filters.Field(True)
    is_default: filters.Boolean
    model: ModelFilter | None
    scenario: ScenarioFilter | None

    class Config:
        sqla_model = Run

    def filter_default_only(self, exc, c, v, **kwargs):
        if v:
            return exc.where(Run.is_default)
        else:
            return exc

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        return exc
