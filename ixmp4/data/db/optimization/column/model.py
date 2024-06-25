from typing import ClassVar

from sqlalchemy import UniqueConstraint

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

    table__id: types.Mapped[int] = db.Column(
        db.Integer, db.ForeignKey("optimization_table.id"), index=True
    )
    indexset: types.Mapped[IndexSet] = db.relationship(single_parent=True)
    constrained_to_indexset: types.Mapped[int] = db.Column(
        db.Integer, db.ForeignKey("optimization_indexset.id"), index=True
    )

    # Currently not in use:
    unique: types.Boolean = db.Column(db.Boolean, default=True)

    __table_args__ = (UniqueConstraint("name", "table__id"),)
