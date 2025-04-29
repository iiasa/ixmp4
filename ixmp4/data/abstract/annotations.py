from collections.abc import Iterable

# TODO Use `type` when dropping Python 3.11
from typing import TypeAlias

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import NotRequired, TypedDict

PrimitiveTypes: TypeAlias = bool | float | int | str
PrimitiveIterableTypes: TypeAlias = (
    Iterable[bool] | Iterable[float] | Iterable[int] | Iterable[str]
)

IntFilterAlias: TypeAlias = int | Iterable[int]
StrFilterAlias: TypeAlias = str | Iterable[str]
DefaultFilterAlias: TypeAlias = IntFilterAlias | StrFilterAlias
OptimizationFilterAlias: TypeAlias = dict[str, DefaultFilterAlias | None]

# NOTE If you want to be nitpicky, you could argue that `timeseries` have an additional
# `variable` filter, which is not clear from this Alias used for both. However,
# `variable` only adds more of the same types and we only use this for casting, so we
# are fine *for now*.
IamcFilterAlias: TypeAlias = dict[
    str,
    bool | DefaultFilterAlias | dict[str, DefaultFilterAlias] | None,
]
IamcObjectFilterAlias: TypeAlias = dict[
    str,
    DefaultFilterAlias
    | dict[
        str,
        dict[str, DefaultFilterAlias | IamcFilterAlias],
    ]
    | bool
    | None,
]


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


class IamcTimeseriesFilter(HasNameFilter, total=False):
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


class HasPaginationArgs(TypedDict, total=False):
    limit: int | None
    offset: int | None


class HasTransactionIdFilter(TypedDict, total=False):
    transaction_id: int | None
    transaction_id__in: Iterable[int]
    transaction_id__gt: int
    transaction_id__lt: int
    transaction_id__gte: int
    transaction_id__lte: int
    transaction__id: int | None
    transaction__id__in: Iterable[int]
    transaction__id__gt: int
    transaction__id__lt: int
    transaction__id__gte: int
    transaction__id__lte: int


class TabulateVersionsKwargs(HasPaginationArgs, total=False):
    transaction__id: NotRequired[int]


class TabulateRunMetaVersionsKwargs(TabulateVersionsKwargs):
    run__id: NotRequired[int]


class TabulateDatapointVersionsKwargs(TabulateVersionsKwargs):
    run__id: NotRequired[int]
