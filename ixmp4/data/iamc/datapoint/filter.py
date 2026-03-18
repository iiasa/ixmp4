from typing import Annotated, cast

from typing_extensions import TypedDict

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.db import Run
from ixmp4.data.versions.filter import VersionFilter

from .db import DataPoint, DataPointVersion


class DataPointFilter(iamc.DataPointFilter, total=False):
    region: Annotated[base.RegionFilter, (DataPoint.timeseries, TimeSeries.region)]
    variable: Annotated[
        iamc.VariableFilter, (DataPoint.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (DataPoint.timeseries, TimeSeries.unit)]
    run: Annotated[base.RunFilter, (DataPoint.timeseries, TimeSeries.run)]
    model: Annotated[
        base.ModelFilter, (DataPoint.timeseries, TimeSeries.run, Run.model)
    ]
    scenario: Annotated[
        base.ScenarioFilter, (DataPoint.timeseries, TimeSeries.run, Run.scenario)
    ]


class DataPointVersionFilter(iamc.DataPointFilter, VersionFilter, total=False):
    timeseries: Annotated[iamc.TimeSeriesFilter, (DataPointVersion.timeseries)]


class FacadeStepYearFilter(TypedDict, total=False):
    year: int
    year__lte: int
    year__lt: int
    year__gte: int
    year__gt: int
    year__in: list[int]


class FacadeStepCategoryFilter(TypedDict, total=False):
    category: str
    category__in: list[str]


class FacadeDataPointFilter(
    DataPointFilter,
    FacadeStepYearFilter,
    FacadeStepCategoryFilter,
    total=False,
):
    pass


facade_to_data_map = {
    "year": "step_year",
    "category": "step_category",
    "datetime": "step_datetime",
}


def facade_to_data_filter(filter: FacadeDataPointFilter) -> DataPointFilter:
    converted = {}
    for key in filter.keys():
        for fname, dname in facade_to_data_map.items():
            if key.startswith(fname):
                converted[key.replace(fname, dname)] = filter.get(key)
                break
        else:
            converted[key] = filter.get(key)
    return cast(DataPointFilter, converted)
