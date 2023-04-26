# flake8: noqa
from ..base import (
    BaseModel as RootBaseModel,
    Retriever,
    Creator,
    Deleter,
    Selecter,
    Lister,
    Tabulator,
    Enumerator,
    BulkUpserter,
    BulkDeleter,
)


class BaseModel(RootBaseModel):
    __abstract__ = True
    table_prefix = "iamc_"
