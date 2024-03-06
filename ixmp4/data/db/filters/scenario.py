from typing import ClassVar

from ixmp4.db import filters, utils

from .. import Run, Scenario


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    sqla_model: ClassVar[type] = Scenario

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Scenario):
            exc = exc.join(Scenario, Run.scenario)
        return exc
