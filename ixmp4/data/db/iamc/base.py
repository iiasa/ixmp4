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
    VersionManager,
)


class BaseModel(RootBaseModel):
    __abstract__ = True
    table_prefix = "iamc_"
