from typing import ClassVar

from ixmp4.core.exceptions import IxmpError
from ixmp4.data import types

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
