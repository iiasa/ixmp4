# flake8: noqa
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
    TimestampMixin,
)


class BaseModel(RootBaseModel, TimestampMixin):
    __abstract__ = True
    table_prefix = "optimization_"
