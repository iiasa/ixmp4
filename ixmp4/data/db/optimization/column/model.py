from typing import ClassVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db.optimization.indexset import IndexSet

from .. import base


class Column(base.BaseModel):
    NotFound: ClassVar = abstract.Column.NotFound
    NotUnique: ClassVar = abstract.Column.NotUnique
    DeletionPrevented: ClassVar = abstract.Column.DeletionPrevented

    # Currently not in use:
    dtype: types.String = db.Column(
        db.String(255), nullable=False, unique=False
    )  # pandas dtype

    equation__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("optimization_equation.id"), nullable=True
    )
    parameter__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("optimization_parameter.id"), nullable=True
    )
    table__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("optimization_table.id"), nullable=True
    )
    # TODO ...
    variable__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("optimization_optimizationvariable.id"), nullable=True
    )

    indexset: types.Mapped[IndexSet] = db.relationship(single_parent=True)
    constrained_to_indexset: types.Integer = db.Column(
        db.Integer, db.ForeignKey("optimization_indexset.id"), index=True
    )

    # Currently not in use:
    unique: types.Boolean = db.Column(db.Boolean, default=True)

    __table_args__ = (db.UniqueConstraint("name", "table__id"),)
