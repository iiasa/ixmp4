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


class StepCategoryFilter(TypedDict, total=False):
    step_category: str
    step_category__in: list[str]


class UnitIdFilter(TypedDict, total=False):
    unit__id: int
    unit__id__lte: int
    unit__id__lt: int
    unit__id__gte: int
    unit__id__gt: int
    unit__id__in: list[int]


class VariableIdFilter(TypedDict, total=False):
    variable__id: int
    variable__id__lte: int
    variable__id__lt: int
    variable__id__gte: int
    variable__id__gt: int
    variable__id__in: list[int]


class VariableFilter(IdFilter, NameFilter, total=False):
    pass


class MeasurandFilter(IdFilter, UnitIdFilter, VariableIdFilter, total=False):
    pass


class TimeSeriesFilter(IdFilter, RunIdFilter, total=False):
    pass


class DataPointFilter(
    IdFilter,
    DataPointTypeFilter,
    StepYearFilter,
    StepCategoryFilter,
    TimeSeriesIdFilter,
    ValueFilter,
    total=False,
):
    pass
