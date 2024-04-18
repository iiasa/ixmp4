# flake8: noqa
from ..base import BaseModel as RootBaseModel
from ..base import (
    BulkDeleter,
    BulkUpserter,
    Creator,
    Deleter,
    Enumerator,
    Lister,
    OptimizationDataMixin,
    OptimizationNameMixin,
    Retriever,
    RunIDMixin,
    Selecter,
    Tabulator,
    TimestampMixin,
    UniqueNameRunIDMixin,
)


class BaseModel(RootBaseModel, OptimizationNameMixin, TimestampMixin):
    __abstract__ = True
    table_prefix = "optimization_"
