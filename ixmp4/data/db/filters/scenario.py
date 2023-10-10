from typing import ClassVar

from ixmp4.db import filters

from .. import Run, Scenario


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id | None = filters.Field(None)
    name: filters.String | None = filters.Field(None)

    sqla_model: ClassVar[type] = Scenario

    def join(self, exc, **kwargs):
        return exc.join(Scenario, Run.scenario)
