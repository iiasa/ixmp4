from collections.abc import Mapping, Sequence
from typing import Annotated, Any, cast

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc
from ixmp4.data.filters.facade import (
    FilterValueTransformer,
    convert_facade_filter,
    make_mapping_transformer,
)
from ixmp4.data.iamc.timeseries.db import TimeSeries
from ixmp4.data.run.filter import (
    FacadeRunFilter,
    RunFilter,
)
from ixmp4.data.run.filter import (
    facade_to_data_filter as run_facade_to_data_filter,
)
from ixmp4.data.versions.filter import VersionFilter

from .db import Region


class IamcRegionFilter(base.RegionFilter, total=False):
    variable: Annotated[
        iamc.VariableFilter,
        (Region.timeseries, TimeSeries.variable),
    ]
    unit: Annotated[base.UnitFilter, (Region.timeseries, TimeSeries.unit)]
    run: Annotated[RunFilter, (Region.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    region_ids_with_timeseries = sa.select(sa.distinct(TimeSeries.region__id))
    if value is True or value == {}:
        return exc.where(Region.id.in_(region_ids_with_timeseries))
    elif value is False:
        return exc.where(Region.id.notin_(region_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc


class RegionFilter(base.RegionFilter, total=False):
    iamc: Annotated[IamcRegionFilter | bool | None, filter_by_iamc]


class RegionVersionFilter(VersionFilter, base.RegionFilter, total=False):
    pass


class FacadeIamcRegionFilter(base.RegionFilter, total=False):
    variable: iamc.VariableFilter
    unit: base.UnitFilter
    run: FacadeRunFilter


IAMC_FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def iamc_facade_to_data_filter(filter_values: Mapping[str, Any]) -> IamcRegionFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=IAMC_FACADE_FILTER_TRANSFORMERS,
    )
    return cast(IamcRegionFilter, converted)


class FacadeRegionFilter(base.RegionFilter, total=False):
    iamc: FacadeIamcRegionFilter | bool | None


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "iamc": (make_mapping_transformer(iamc_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> RegionFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(RegionFilter, converted)
