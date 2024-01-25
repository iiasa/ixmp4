from typing import Any, ClassVar

import pandas as pd
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base


class Table(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    constrained_to_indexsets: ClassVar[list[str] | None] = None

    name: types.String = db.Column(db.String(255), nullable=False, unique=False)
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    columns: types.Mapped[list["Column"]] = db.relationship()

    # NOTE: Could create a RunMixin for all optimization models, see
    # https://docs.sqlalchemy.org/en/20/orm/declarative_mixins.html#mixing-in-relationships
    run__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("run.id"), nullable=False, index=True
    )

    __table_args__ = (UniqueConstraint(name, "run__id"),)

    # NOTE: This could probably also be a Mixin across almost all models
    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)

    def collect_indexsets_to_check(self) -> dict[str, Any]:
        collection: dict[str, Any] = {}
        for column in self.columns:
            collection[column.name] = column.indexset.elements
        return collection

    @validates("data")
    def validate_data(self, key, data: dict[str, Any] | pd.DataFrame):
        if isinstance(data, dict):
            data = pd.DataFrame.from_dict(data)

        # TODO for all of the following, we might want to create unique exceptions
        # TODO: we could make this more specific maybe by pointing to the missing values
        if data.notna().any():
            raise ValueError(
                "Table.data is missing values, please make sure it does "
                "not contain NaN, either!"
            )
        # TODO we can make this more specific e.g. highlighting all duplicate rows via
        # pd.DataFrame.duplicated(keep="False")
        if data.value_counts().max() > 1:
            raise ValueError("Table.data contains duplicate rows!")

        # TODO can we make this more specific? Iterating over columns; if any is False,
        # return its name or something?
        limited_to_indexsets = self.collect_indexsets_to_check()
        if not data.isin(limited_to_indexsets).all():
            raise ValueError(
                "Table.data contains values that are not allowed as per "
                "the IndexSets it is constrained to!"
            )

        return data.to_dict()
