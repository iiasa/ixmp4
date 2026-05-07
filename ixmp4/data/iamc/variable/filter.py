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
from ixmp4.data.versions.filter import VersionFilter

from .db import Variable


def filter_by_run(
    exc: sa.Select[Any],
    value: dict[str, Any] | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any]:
    if value is None:
        return exc
    else:
        assert repo.filter is not None
        for field_name, sub_value in value.items():
            exc = repo.filter.where_filter_item_matches(
                repo, exc, ("run", field_name), sub_value
            )
        return exc.distinct()


class VariableFilter(iamc.VariableFilter, total=False):
    unit: Annotated[base.UnitFilter, (Variable.timeseries, TimeSeries.unit)]
    region: Annotated[base.RegionFilter, (Variable.timeseries, TimeSeries.region)]
    run: Annotated[
        RunFilter | None, (Variable.timeseries, TimeSeries.run), filter_by_run
    ]


class VariableVersionFilter(VersionFilter, iamc.VariableFilter, total=False):
    pass


class FacadeVariableFilter(iamc.VariableFilter, total=False):
    unit: base.UnitFilter
    region: base.RegionFilter
    run: FacadeRunFilter | None


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> VariableFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(VariableFilter, converted)
