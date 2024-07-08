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
    __abstract__ = True
    table_prefix = "optimization_"

    name: types.Name
