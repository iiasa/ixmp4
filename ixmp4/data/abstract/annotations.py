from collections.abc import Iterable

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict


class HasIdFilter(TypedDict, total=False):
    id: int
    id__in: Iterable[int]


class HasNameFilter(TypedDict, total=False):
    name: str | None
    name__in: Iterable[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str


class HasHierarchyFilter(TypedDict, total=False):
    hierarchy: str | None
    hierarchy__in: Iterable[str]
    hierarchy__like: str
    hierarchy__ilike: str
    hierarchy__notlike: str
    hierarchy__notilike: str


class HasRunIdFilter(TypedDict, total=False):
    run_id: int | None
    run_id__in: Iterable[int]
    run_id__gt: int
    run_id__lt: int
    run_id__gte: int
    run_id__lte: int
    run__id: int | None
    run__id__in: Iterable[int]
    run__id__gt: int
    run__id__lt: int
    run__id__gte: int
    run__id__lte: int


class HasUnitIdFilter(TypedDict, total=False):
    unit_id: int | None
    unit_id__in: Iterable[int]
    unit_id__gt: int
    unit_id__lt: int
    unit_id__gte: int
    unit_id__lte: int
    unit__id: int | None
    unit__id__in: Iterable[int]
    unit__id__gt: int
    unit__id__lt: int
    unit__id__gte: int
    unit__id__lte: int


class HasRegionFilter(HasHierarchyFilter, HasIdFilter, HasNameFilter, total=False): ...


class HasModelFilter(HasIdFilter, HasNameFilter, total=False): ...


class HasScenarioFilter(HasIdFilter, HasNameFilter, total=False): ...


class HasUnitFilter(HasIdFilter, HasNameFilter, total=False): ...


class HasVariableFilter(HasIdFilter, HasNameFilter, total=False): ...


class HasRunFilter(HasIdFilter, total=False):
    version: int | None
    default_only: bool
    is_default: bool | None
    model: HasModelFilter | None
    scenario: HasScenarioFilter | None


class IamcScenarioFilter(TypedDict, total=False):
    region: HasRegionFilter | None
    variable: HasVariableFilter | None
    unit: HasUnitFilter | None
    run: HasRunFilter


class IamcUnitFilter(TypedDict, total=False):
    region: HasRegionFilter | None
    variable: HasVariableFilter | None
    run: HasRunFilter


class IamcRunFilter(TypedDict, total=False):
    region: HasRegionFilter | None
    variable: HasVariableFilter | None
    unit: HasUnitFilter | None


class IamcRegionFilter(TypedDict, total=False):
    variable: HasVariableFilter | None
    unit: HasUnitFilter | None
    run: HasRunFilter


class IamcModelFilter(TypedDict, total=False):
    region: HasRegionFilter | None
    variable: HasVariableFilter | None
    unit: HasUnitFilter | None
    run: HasRunFilter
