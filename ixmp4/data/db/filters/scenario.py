from typing import ClassVar

from ixmp4.db import filters

from .. import Run, Scenario


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    sqla_model: ClassVar[type] = Scenario

    def join(self, exc, **kwargs):
        return exc.join(Scenario, Run.scenario)
