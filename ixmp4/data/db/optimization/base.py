from ixmp4.data import types

from ..base import BaseModel as RootBaseModel
from ..base import (
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    HasCreationInfo,
    Lister,
    Retriever,
    Selecter,
    Tabulator,
)


class BaseModel(RootBaseModel, HasCreationInfo):
    __abstract__ = True
    table_prefix = "optimization_"

    name: types.Name
