from typing import ClassVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db.unit import Unit

from .. import base


class Scalar(base.BaseModel):
    NotFound: ClassVar = abstract.Scalar.NotFound
    NotUnique: ClassVar = abstract.Scalar.NotUnique
    DeletionPrevented: ClassVar = abstract.Scalar.DeletionPrevented

    run__id: types.RunId

    value: types.Float = db.Column(db.Float, nullable=True, unique=False)

    unit: types.Mapped[Unit | None] = db.relationship()
    unit__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("unit.id"), index=True
    )

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
