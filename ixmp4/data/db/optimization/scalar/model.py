from typing import ClassVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db import versions
from ixmp4.data.db.unit import Unit

from .. import base


class Scalar(base.BaseModel):
    __tablename__ = "opt_sca"

    NotFound: ClassVar = abstract.Scalar.NotFound
    NotUnique: ClassVar = abstract.Scalar.NotUnique
    DeletionPrevented: ClassVar = abstract.Scalar.DeletionPrevented

    run__id: types.RunId

    value: types.Float = db.Column(db.Float, nullable=True, unique=False)

    unit: types.Mapped[Unit] = db.relationship()
    unit__id: types.Mapped[int] = db.Column(
        db.Integer, db.ForeignKey("unit.id"), index=True
    )

    updateable_columns = ["value", "unit__id"]

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class ScalarVersion(versions.DefaultVersionModel):
    __tablename__ = "opt_sca_version"

    name: types.String = db.Column(db.String(255), nullable=False)
    run__id: db.MappedColumn[int] = db.Column(db.Integer, nullable=False, index=True)
    value = db.Column(db.Float, nullable=True, unique=False)
    unit__id: db.MappedColumn[int] = db.Column(db.Integer, index=True)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Scalar.__table__, ScalarVersion.__table__
)
