from collections.abc import Iterable
from typing import ClassVar

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import TypedDict

from ixmp4.core.exceptions import IxmpError
from ixmp4.data import types
from ixmp4.db.filters import BaseFilter

from .. import mixins
from ..base import BaseModel as RootBaseModel
from ..base import (
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    Lister,
    Retriever,
    Selecter,
    Tabulator,
)


class BaseModel(RootBaseModel, mixins.HasCreationInfo):
    # NOTE: only subclasses storing data actually define this!
    DataInvalid: ClassVar[type[IxmpError]]

    __abstract__ = True
    table_prefix = "optimization_"

    name: types.Name


class EnumerateKwargs(TypedDict, total=False):
    _filter: BaseFilter
    name: str | None
    name__in: Iterable[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str
