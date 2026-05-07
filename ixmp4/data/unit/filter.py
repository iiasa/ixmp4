from collections.abc import Mapping, Sequence
from typing import Annotated, Any, cast

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
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

from .db import Unit


class IamcUnitFilter(base.UnitFilter, total=False):
    variable: Annotated[iamc.VariableFilter, (Unit.timeseries, TimeSeries.variable)]
    region: Annotated[base.RegionFilter, (Unit.timeseries, TimeSeries.region)]
    run: Annotated[RunFilter, (Unit.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    if value is True or value == {}:
        return exc.where(Unit.timeseries.any())
    elif value is False:
        return exc.where(~Unit.timeseries.any())
    elif value is None:
        return exc
    else:
        return exc


class UnitFilter(base.UnitFilter, total=False):
    iamc: Annotated[IamcUnitFilter | bool | None, filter_by_iamc]


class FacadeIamcUnitFilter(base.UnitFilter, total=False):
    variable: iamc.VariableFilter
    region: base.RegionFilter
    run: FacadeRunFilter


IAMC_FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def iamc_facade_to_data_filter(filter_values: Mapping[str, Any]) -> IamcUnitFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=IAMC_FACADE_FILTER_TRANSFORMERS,
    )
    return cast(IamcUnitFilter, converted)


class FacadeUnitFilter(base.UnitFilter, total=False):
    iamc: FacadeIamcUnitFilter | bool | None


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "iamc": (make_mapping_transformer(iamc_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> UnitFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(UnitFilter, converted)
