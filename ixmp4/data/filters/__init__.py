from typing import Annotated, Any

import sqlalchemy as sa
from typing_extensions import TypedDict

from ixmp4.data.run.db import Run

from .base import HierarchyFilter as HierarchyFilter
from .base import IdFilter as IdFilter
from .base import NameFilter as NameFilter
from .base import RunIdFilter as RunIdFilter
from .base import TransactionIdFilter as TransactionIdFilter
from .iamc import TimeSeriesIdFilter as TimeSeriesIdFilter


class UnitFilter(IdFilter, NameFilter, total=False):
    pass


class RegionFilter(IdFilter, NameFilter, HierarchyFilter, total=False):
    pass


class ScenarioFilter(IdFilter, NameFilter, total=False):
    pass


class ModelFilter(IdFilter, NameFilter, total=False):
    pass


class CheckpointFilter(IdFilter, RunIdFilter, TransactionIdFilter, total=False):
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


class RunVersionFilter(TypedDict, total=False):
    version: int
    version__in: list[int]


class RunFilter(IdFilter, RunVersionFilter, total=False):
    is_default: bool
    default_only: Annotated[bool, filter_by_default_only]


# Docs


class DimensionIdFilter(TypedDict, total=False):
    dimension__id: int
    dimension__id__in: list[int]


class DocsFilter(IdFilter, DimensionIdFilter, total=False):
    pass
