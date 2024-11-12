# TODO Import this from typing when dropping Python 3.11
from collections.abc import Iterable

from typing_extensions import TypedDict

from .column import Column
from .equation import Equation, EquationRepository
from .indexset import IndexSet, IndexSetRepository
from .parameter import Parameter, ParameterRepository
from .scalar import Scalar, ScalarRepository
from .table import Table, TableRepository
from .variable import Variable, VariableRepository


class EnumerateKwargs(TypedDict, total=False):
    id: int | None
    id__in: Iterable[int]
    # name: str | None
    name__in: Iterable[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str
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
