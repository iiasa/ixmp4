from collections.abc import Iterable, Mapping, Sequence
from typing import Annotated, Any, cast

from typing_extensions import TypedDict

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.filters.facade import (
    FilterValueTransformer,
    convert_facade_filter,
    make_iterable_str_in_transformer,
    make_mapping_transformer,
    make_str_like_transformer,
)
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.db import Run
from ixmp4.data.run.filter import FacadeRunFilter
from ixmp4.data.run.filter import facade_to_data_filter as run_facade_to_data_filter
from ixmp4.data.versions.filter import VersionFilter

from .db import DataPoint, DataPointVersion


class DataPointFilter(iamc.DataPointFilter, total=False):
    region: Annotated[base.RegionFilter, (DataPoint.timeseries, TimeSeries.region)]
    variable: Annotated[
        iamc.VariableFilter, (DataPoint.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (DataPoint.timeseries, TimeSeries.unit)]
    meta: Annotated[
        base.RunMetaEntryFilter,
        (DataPoint.timeseries, TimeSeries.run, Run.meta),
    ]
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
    iamc.DataPointFilter,
    FacadeStepYearFilter,
    FacadeStepCategoryFilter,
    total=False,
):
    region: base.RegionFilter | str | Iterable[str]
    unit: base.UnitFilter | str | Iterable[str]
    variable: iamc.VariableFilter | str | Iterable[str]
    model: base.ModelFilter | str | Iterable[str]
    scenario: base.ScenarioFilter | str | Iterable[str]
    meta: base.RunMetaEntryFilter | str | Iterable[str]
    run: FacadeRunFilter


FACADE_FILTER_KEY_MAP = {
    "year": "step_year",
    "category": "step_category",
    "datetime": "step_datetime",
}


NAME_FILTER_TRANSFORMERS: tuple[FilterValueTransformer, ...] = (
    make_str_like_transformer("name"),
    make_iterable_str_in_transformer("name"),
)


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "region": NAME_FILTER_TRANSFORMERS,
    "unit": NAME_FILTER_TRANSFORMERS,
    "variable": NAME_FILTER_TRANSFORMERS,
    "model": NAME_FILTER_TRANSFORMERS,
    "scenario": NAME_FILTER_TRANSFORMERS,
    "meta": (
        make_str_like_transformer("key"),
        make_iterable_str_in_transformer("key"),
    ),
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def facade_to_data_filter(
    filter_values: Mapping[str, Any],
) -> DataPointFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map=FACADE_FILTER_KEY_MAP,
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(DataPointFilter, converted)
