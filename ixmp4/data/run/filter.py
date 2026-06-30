from collections.abc import Iterable, Mapping, Sequence
from typing import Annotated, Any, cast

import sqlalchemy as sa
from toolkit.db.repositories import BaseRepository
from typing_extensions import TypedDict

from ixmp4.data import filters as base
from ixmp4.data.filters import iamc as iamc
from ixmp4.data.filters.facade import (
    FilterValueTransformer,
    convert_facade_filter,
    make_iterable_str_in_transformer,
    make_str_like_transformer,
)
from ixmp4.data.iamc.measurand.db import Measurand
from ixmp4.data.iamc.timeseries.db import TimeSeries

from .db import Run


class IamcRunFilter(TypedDict, total=False):
    variable: Annotated[
        iamc.VariableFilter,
        (Run.timeseries, TimeSeries.measurand, Measurand.variable),
    ]
    unit: Annotated[base.UnitFilter, (Run.timeseries, TimeSeries.unit)]
    region: Annotated[base.RegionFilter, (Run.timeseries, TimeSeries.region)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    run_ids_with_timeseries = sa.select(sa.distinct(TimeSeries.run__id))
    if value is True or value == {}:
        return exc.where(Run.id.in_(run_ids_with_timeseries))
    elif value is False:
        return exc.where(Run.id.notin_(run_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc


class RunFilter(base.RunFilter, total=False):
    model: Annotated[base.ModelFilter, Run.model]
    scenario: Annotated[base.ScenarioFilter, Run.scenario]
    meta: Annotated[base.RunMetaEntryFilter, Run.meta]
    iamc: Annotated[IamcRunFilter | bool | None, filter_by_iamc]


class FacadeRunFilter(base.RunFilter, total=False):
    model: base.ModelFilter | str | Iterable[str]
    scenario: base.ScenarioFilter | str | Iterable[str]
    meta: base.RunMetaEntryFilter | str | Iterable[str]
    iamc: IamcRunFilter | bool | None


NAME_FILTER_TRANSFORMERS: tuple[FilterValueTransformer, ...] = (
    make_str_like_transformer("name"),
    make_iterable_str_in_transformer("name"),
)


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "model": NAME_FILTER_TRANSFORMERS,
    "scenario": NAME_FILTER_TRANSFORMERS,
    "meta": (
        make_str_like_transformer("key"),
        make_iterable_str_in_transformer("key"),
    ),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> RunFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(RunFilter, converted)
