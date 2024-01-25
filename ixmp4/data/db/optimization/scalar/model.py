from typing import ClassVar

from sqlalchemy import UniqueConstraint

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db.unit import Unit

from .. import base


class Scalar(base.BaseModel):
    NotFound: ClassVar = abstract.Scalar.NotFound
    NotUnique: ClassVar = abstract.Scalar.NotUnique
    DeletionPrevented: ClassVar = abstract.Scalar.DeletionPrevented

    name: types.String = db.Column(db.String(255), nullable=False, unique=False)
    value: types.Float = db.Column(db.Float, nullable=True, unique=False)

    unit: types.Mapped[Unit | None] = db.relationship()
    unit__id: types.Mapped[int | None] = db.Column(
        db.Integer, db.ForeignKey("unit.id"), index=True
    )

    run__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("run.id"), nullable=False, index=True
    )

    __table_args__ = (UniqueConstraint(name, run__id),)

    created_at: types.DateTime = db.Column(db.DateTime, nullable=True)
    created_by: types.String = db.Column(db.String(255), nullable=True)
