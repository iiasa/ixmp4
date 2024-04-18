# flake8: noqa
from ..base import BaseModel as RootBaseModel
from ..base import (
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    Lister,
    NameMixin,
    Retriever,
    Selecter,
    Tabulator,
    TimestampMixin,
)


class BaseModel(RootBaseModel):
    __abstract__ = True
    table_prefix = "iamc_"
