from ixmp4.db import filters

from .. import Run, Scenario


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Scenario

    def join(self, exc, **kwargs):
        return exc.join(Scenario, Run.scenario)
