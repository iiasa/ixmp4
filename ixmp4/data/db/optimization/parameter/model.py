from typing import Any, ClassVar

import pandas as pd
from sqlalchemy import Column as sqlaColumn
from sqlalchemy import Table, UniqueConstraint
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db.unit import Unit

from .. import Column, base

# Many Parameters can refer to many Units
# note for a Core table, we use the sqlalchemy.Column construct,
# not sqlalchemy.orm.mapped_column

parameter_unit_association_table = Table(
    "optimization_parameter_unit_association_table",
    base.BaseModel.metadata,
    sqlaColumn(
        "parameter_id", db.ForeignKey("optimization_parameter.id"), primary_key=True
    ),
    sqlaColumn("unit_id", db.ForeignKey("unit.id"), primary_key=True),
)


class Parameter(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    name: types.String = db.Column(db.String(255), nullable=False, unique=False)
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    values: types.JsonList = db.Column(db.JsonType, nullable=False, default=[])
    units: types.Mapped[list["Unit"]] = db.relationship(
        secondary=parameter_unit_association_table
    )

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

    def collect_indexsets_to_check(self) -> dict[str, list[Any]]:
        """Creates a {key:value} dict from linked Column.names and their
        IndexSet.elements."""
        return {column.name: column.indexset.elements for column in self.columns}

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        data_frame: pd.DataFrame = pd.DataFrame.from_dict(data)
        # TODO for all of the following, we might want to create unique exceptions
        # Could we make both more specific by specifiying missing/extra columns?
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
                "Parameter.data is missing values, please make sure it does "
                "not contain None or NaN, either!"
            )
        # We can make this more specific e.g. highlighting all duplicate rows via
        # pd.DataFrame.duplicated(keep="False")
        if data_frame.value_counts().max() > 1:
            raise ValueError("Parameter.data contains duplicate rows!")

        # Can we make this more specific? Iterating over columns; if any is False,
        # return its name or something?
        limited_to_indexsets = self.collect_indexsets_to_check()
        if not data_frame.isin(limited_to_indexsets).all(axis=None):
            raise ValueError(
                "Parameter.data contains values that are not allowed as per the "
                "IndexSets and Columns it is constrained to!"
            )

        return data_frame.to_dict(orient="list")
