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
from ixmp4.data.run.db import Run
from ixmp4.data.run.filter import (
    FacadeRunFilter,
    RunFilter,
)
from ixmp4.data.run.filter import (
    facade_to_data_filter as run_facade_to_data_filter,
)

from .db import Scenario


class IamcScenarioFilter(base.ScenarioFilter, total=False):
    variable: Annotated[
        iamc.VariableFilter, (Scenario.runs, Run.timeseries, TimeSeries.variable)
    ]
    unit: Annotated[base.UnitFilter, (Scenario.runs, Run.timeseries, TimeSeries.unit)]
    region: Annotated[
        base.RegionFilter, (Scenario.runs, Run.timeseries, TimeSeries.region)
    ]
    run: Annotated[RunFilter, (Scenario.runs, Run.timeseries, TimeSeries.run)]


def filter_by_iamc(
    exc: sa.Select[Any] | sa.Update | sa.Delete,
    value: dict[str, Any] | bool | None,
    *,
    schema: type[Any],
    repo: BaseRepository[Any],
) -> sa.Select[Any] | sa.Update | sa.Delete:
    scen_ids_with_timeseries = (
        sa.select(Run.scenario__id)
        .where(Run.id.in_(sa.select(TimeSeries.run__id).distinct()))
        .distinct()
    )

    if value is True or value == {}:
        return exc.where(Scenario.id.in_(scen_ids_with_timeseries))
    elif value is False:
        return exc.where(~Scenario.id.in_(scen_ids_with_timeseries))
    elif value is None:
        return exc
    else:
        return exc


class ScenarioFilter(base.ScenarioFilter, total=False):
    iamc: Annotated[IamcScenarioFilter | bool | None, filter_by_iamc]


class FacadeIamcScenarioFilter(base.ScenarioFilter, total=False):
    variable: iamc.VariableFilter
    unit: base.UnitFilter
    region: base.RegionFilter
    run: FacadeRunFilter


IAMC_FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def iamc_facade_to_data_filter(
    filter_values: Mapping[str, Any],
) -> IamcScenarioFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=IAMC_FACADE_FILTER_TRANSFORMERS,
    )
    return cast(IamcScenarioFilter, converted)


class FacadeScenarioFilter(base.ScenarioFilter, total=False):
    iamc: FacadeIamcScenarioFilter | bool | None


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "iamc": (make_mapping_transformer(iamc_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> ScenarioFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(ScenarioFilter, converted)
