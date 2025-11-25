from typing import TypedDict

from .base import (
    IdFilter,
    NameFilter,
    RunIdFilter,
    TimeSeriesIdFilter,
    ValueFilter,
)


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
