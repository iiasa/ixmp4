from typing import ClassVar

from ixmp4.data.db.iamc.datapoint import get_datapoint_model
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.data.db.iamc.timeseries import TimeSeries
from ixmp4.data.db.iamc.variable import Variable
from ixmp4.data.db.model import Model
from ixmp4.data.db.region import Region
from ixmp4.data.db.run import Run
from ixmp4.data.db.scenario import Scenario
from ixmp4.data.db.unit import Unit
from ixmp4.db import filters, utils


class RegionFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    name: filters.String
    hierarchy: filters.String

    sqla_model: ClassVar[type] = Region

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Region):
            exc = exc.join(Region, TimeSeries.region)
        return exc


class UnitFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    name: filters.String

    sqla_model: ClassVar[type] = Unit

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        if not utils.is_joined(exc, Unit):
            exc = exc.join(Unit, Measurand.unit)

        return exc


class VariableFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    name: filters.String

    sqla_model: ClassVar[type] = Variable

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Measurand):
            exc = exc.join(Measurand, TimeSeries.measurand)
        if not utils.is_joined(exc, Variable):
            exc = exc.join(Variable, Measurand.variable)
        return exc


class ModelFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    name: filters.String

    sqla_model: ClassVar[type] = Model

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        if not utils.is_joined(exc, Model):
            exc = exc.join(Model, Run.model)
        return exc


class ScenarioFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    name: filters.String

    sqla_model: ClassVar[type] = Scenario

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, onclause=model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        if not utils.is_joined(exc, Scenario):
            exc = exc.join(Scenario, onclause=Run.scenario__id == Scenario.id)
        return exc


class RunFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    id: filters.Id
    default_only: filters.Boolean = filters.Field(True)

    sqla_model: ClassVar[type] = Run

    def join(self, exc, session):
        model = get_datapoint_model(session)
        if not utils.is_joined(exc, TimeSeries):
            exc = exc.join(TimeSeries, model.time_series__id == TimeSeries.id)
        if not utils.is_joined(exc, Run):
            exc = exc.join(Run, TimeSeries.run)
        return exc

    def filter_default_only(self, exc, c, v, **kwargs):
        if v:
            return exc.where(Run.is_default)
        else:
            return exc


class DataPointFilter(filters.BaseFilter, metaclass=filters.FilterMeta):
    """This class is used for filtering data points

    All parameters are optional. Use the field name (or the field alias)
    directly for equality comparisons. For performing an SQL IN operation
    use the field name followed by a double underscore and *in*.

    Parameters
    ----------
    step_year : filters.Integer, Optional
        Filter for data point year, can also be called with "year"
    time_series__id : filters.Id, Optional
        Filter for the id of the time series, can also be called with "time_series_id"
    region : RegionFilter, Optional
        Filter for either region name or hierarchy
    unit : UnitFilter, Optional
        Filter for unit name
    variable : VariableFilter, Optional
        Filter for variable name
    model : ModelFilter, Optional
        Filter for model name
    scenario : ScenarioFilter, Optional
        Filter for the scenario name
    run : RunFilter, Optional
        Filter for the run, options are id or default_only

    Examples
    --------

    Return all data points for a given year.

    >>> DataPointFilter(year=2020)

    Return all data points a number of years

    >>> DataPointFilter(year__in=[2020, 2025])

    Return all data point that map to a given variable

    >>> DataPointFilter(variable=VariableFilter(name="variable 1"))

    Note that for actual use providing the filter parameters as key word arguments is
    sufficient. Calling an endpoint could look like this:

    >>> filter = {"variable": {"name": "variable 1"}, "year": 2020}
    >>> iamc.tabulate(**filter)
    """

    step_year: filters.Integer = filters.Field(None, alias="year")
    time_series__id: filters.Id = filters.Field(None, alias="time_series_id")
    region: RegionFilter
    unit: UnitFilter
    variable: VariableFilter
    model: ModelFilter
    scenario: ScenarioFilter
    run: RunFilter = filters.Field(RunFilter())
