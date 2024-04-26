from typing import ClassVar

from sqlalchemy import Column as sqlaColumn
from sqlalchemy import Table

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db.unit import Unit

from .. import Column, base

# Many Parameters can refer to many Units
# note for a Core table, we use the sqlalchemy.Column construct,
# not sqlalchemy.orm.mapped_column

# TODO Is this enough/correct? This follows many-to-many currently with units:
# But does that work?

parameter_unit_association_table = Table(
    "optimization_parameter_unit_association_table",
    base.BaseModel.metadata,
    sqlaColumn("parameter__id", db.ForeignKey("optimization_parameter.id")),
    sqlaColumn("unit__id", db.ForeignKey("unit.id")),
)


class Parameter(base.BaseModel, base.OptimizationDataMixin, base.UniqueNameRunIDMixin):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    values: types.JsonList = db.Column(db.JsonType, nullable=False, default=[])
    units: types.Mapped[list["Unit"]] = db.relationship(
        secondary=parameter_unit_association_table
    )
    # TODO: need some kind of primaryjoin adaption and unit_ids so that each unit_id is
    # foreignkeyed to Unit.id correctly

    # TODO Same as in table/model.py
    columns: types.Mapped[list["Column"]] = db.relationship()  # type: ignore
