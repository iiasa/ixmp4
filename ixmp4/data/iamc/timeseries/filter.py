from collections.abc import Mapping, Sequence
from typing import Annotated, Any, cast

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.filters.facade import (
    FilterValueTransformer,
    convert_facade_filter,
    make_mapping_transformer,
)
from ixmp4.data.run.filter import (
    FacadeRunFilter,
)
from ixmp4.data.run.filter import (
    facade_to_data_filter as run_facade_to_data_filter,
)
from ixmp4.data.versions.filter import VersionFilter

from .db import TimeSeries


class TimeSeriesFilter(iamc.TimeSeriesFilter, total=False):
    region: Annotated[base.RegionFilter, TimeSeries.region]
    variable: Annotated[iamc.VariableFilter, TimeSeries.variable]
    unit: Annotated[base.UnitFilter, TimeSeries.unit]
    run: Annotated[base.RunFilter, TimeSeries.run]


class TimeSeriesVersionFilter(VersionFilter, iamc.TimeSeriesFilter, total=False):
    pass


class FacadeTimeSeriesFilter(iamc.TimeSeriesFilter, total=False):
    region: base.RegionFilter
    variable: iamc.VariableFilter
    unit: base.UnitFilter
    run: FacadeRunFilter


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> TimeSeriesFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(TimeSeriesFilter, converted)
