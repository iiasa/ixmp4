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
    TabulateTransactionsKwargs,
    TabulateVersionsKwargs,
    Tabulator,
)


class BaseModel(RootBaseModel):
    __abstract__ = True
