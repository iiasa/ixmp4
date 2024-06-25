from typing import Any, ClassVar

import pandas as pd
from sqlalchemy.orm import Mapped as Mapped
from sqlalchemy.orm import declared_attr, validates

from ixmp4 import db
from ixmp4.data import abstract, types

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

    name: Mapped[db.Name]
    run__id: Mapped[db.RunId]


class OptimizationDataBaseModel(BaseModel):
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    columns: ClassVar[list[abstract.optimization.Column]]

    def collect_indexsets_to_check(self) -> dict[str, Any]:
        """Creates a {key:value} dict from linked Column.names and their
        IndexSet.elements."""
        collection: dict[str, Any] = {}
        for column in self.columns:
            collection[column.name] = column.indexset.elements
        return collection

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        data_frame: pd.DataFrame = pd.DataFrame.from_dict(data)
        # TODO for all of the following, we might want to create unique exceptions
        # Could me make both more specific by specifiying missing/extra columns?
        if len(data_frame.columns) < len(self.columns):
            raise ValueError(
                f"Data is missing for some Columns! \n Data: {data} \n "
                f"Columns: {[column.name for column in self.columns]}"
            )
        elif len(data_frame.columns) > len(self.columns):
            raise ValueError(
                f"Trying to add data to unknown Columns! \n Data: {data} \n "
                f"Columns: {[column.name for column in self.columns]}"
            )

        # We could make this more specific maybe by pointing to the missing values
        if data_frame.isna().any(axis=None):
            raise ValueError(
                "The data is missing values, please make sure it "
                "does not contain None or NaN, either!"
            )
        # We can make this more specific e.g. highlighting all duplicate rows via
        # pd.DataFrame.duplicated(keep="False")
        if data_frame.value_counts().max() > 1:
            raise ValueError("The data contains duplicate rows!")

        # Can we make this more specific? Iterating over columns; if any is False,
        # return its name or something?
        limited_to_indexsets = self.collect_indexsets_to_check()
        if not data_frame.isin(limited_to_indexsets).all(axis=None):
            raise ValueError(
                "The data contains values that are not allowed as per the IndexSets "
                "and Columns it is constrained to!"
            )

        return data_frame.to_dict(orient="list")
