from typing import Annotated, Any, TypedDict

import sqlalchemy as sa

from ixmp4.rewrite.data.run.db import Run


class IdFilter(TypedDict, total=False):
    id: int
    id__in: list[int]


class NameFilter(TypedDict, total=False):
    name: str
    name__in: list[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str


class HierarchyFilter(TypedDict, total=False):
    hierarchy: str
    hierarchy__in: list[str]
    hierarchy__like: str
    hierarchy__ilike: str
    hierarchy__notlike: str
    hierarchy__notilike: str


class ValueFilter(TypedDict, total=False):
    value: float
    value__lte: float
    value__lt: float
    value__gte: float
    value__gt: float
    value__in: list[float]


class RunIdFilter(TypedDict, total=False):
    run__id: int
    run__id__in: list[int]


class TransactionIdFilter(TypedDict, total=False):
    transaction__id: int
    transaction__id__in: list[int]


class TimeSeriesIdFilter(TypedDict, total=False):
    time_series__id: int
    time_series__id__in: list[int]


class DataPointTypeFilter(TypedDict, total=False):
    type: str
    type__in: list[str]


class StepYearFilter(TypedDict, total=False):
    step_year: int
    step_year__lte: int
    step_year__lt: int
    step_year__gte: int
    step_year__gt: int
    step_year__in: list[int]


class VariableFilter(IdFilter, NameFilter, total=False):
    pass


class UnitFilter(IdFilter, NameFilter, total=False):
    pass


class RegionFilter(IdFilter, NameFilter, HierarchyFilter, total=False):
    pass


class ScenarioFilter(IdFilter, NameFilter, total=False):
    pass


class ModelFilter(IdFilter, NameFilter, total=False):
    pass


class TimeSeriesFilter(IdFilter, RunIdFilter, total=False):
    pass


class DataPointFilter(
    IdFilter,
    DataPointTypeFilter,
    StepYearFilter,
    TimeSeriesIdFilter,
    ValueFilter,
    total=False,
):
    pass


class CheckpointFilter(IdFilter, RunIdFilter, TransactionIdFilter, total=True):
    pass


# Meta


class KeyFilter(TypedDict, total=False):
    key: str
    key__in: list[str]


class DtypeFilter(TypedDict, total=False):
    dtype: str
    dtype__in: list[str]


class RunMetaEntryFilter(IdFilter, KeyFilter, RunIdFilter, DtypeFilter, total=False):
    pass


# Run


def filter_by_default_only(
    exc: sa.Select[Any] | sa.Update | sa.Delete, value: bool, **kwargs: Any
) -> sa.Select[Any] | sa.Update | sa.Delete:
    return exc.where(Run.is_default) if value else exc


class VersionFilter(TypedDict, total=False):
    version: int
    version__in: list[int]


class RunFilter(IdFilter, VersionFilter, total=False):
    is_default: bool
    default_only: Annotated[bool, filter_by_default_only]


# Docs


class DimensionIdFilter(TypedDict, total=False):
    dimension__id: int
    dimension__id__in: list[int]


class DocsFilter(IdFilter, DimensionIdFilter, total=False):
    pass
