from typing import TypedDict


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
