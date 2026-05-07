from collections.abc import Mapping, Sequence
from typing import Annotated, Any, cast

from ixmp4.data import filters as base
from ixmp4.data.filters.facade import (
    FilterValueTransformer,
    convert_facade_filter,
    make_mapping_transformer,
)
from ixmp4.data.run.db import Run
from ixmp4.data.run.filter import FacadeRunFilter
from ixmp4.data.run.filter import facade_to_data_filter as run_facade_to_data_filter

from .db import RunMetaEntry


class RunFilter(base.RunFilter, total=False):
    model: Annotated[base.ModelFilter, Run.model]
    scenario: Annotated[base.ScenarioFilter, Run.scenario]


class RunMetaEntryFilter(base.RunMetaEntryFilter, total=False):
    run: Annotated[RunFilter, RunMetaEntry.run]


class FacadeRunMetaEntryFilter(base.RunMetaEntryFilter, total=False):
    run: FacadeRunFilter


FACADE_FILTER_TRANSFORMERS: dict[str, Sequence[FilterValueTransformer]] = {
    "run": (make_mapping_transformer(run_facade_to_data_filter),),
}


def facade_to_data_filter(filter_values: Mapping[str, Any]) -> RunMetaEntryFilter:
    converted = convert_facade_filter(
        filter_values,
        key_map={},
        field_transformers=FACADE_FILTER_TRANSFORMERS,
    )
    return cast(RunMetaEntryFilter, converted)
