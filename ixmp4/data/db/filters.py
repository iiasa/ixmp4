from ixmp4.db import filters, utils

from . import Run, Model, Scenario, Region, Unit, Variable, Measurand, TimeSeries


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Model

    def join(self, exc, **kwargs):
        return exc.join(Model, Run.model)


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Scenario

    def join(self, exc, **kwargs):
        return exc.join(Scenario, Run.scenario)


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
        exc = exc.join(Run, TimeSeries.run)
        return exc


class VariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Variable

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        exc = exc.join(Variable, Measurand.variable)
        return exc


class UnitFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String

    class Config:
        sqla_model = Unit

    def join(self, exc, **kwargs):
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        exc = exc.join(Unit, Measurand.unit)
        return exc


class RegionFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    name: filters.String
    hierarchy: filters.String

    class Config:
        sqla_model = Region

    def join(self, exc, **kwargs):
        exc = exc.join(Region, TimeSeries.region)
        return exc
