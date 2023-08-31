from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db.model.model import Model
from ixmp4.data.db.optimization.indexset import IndexSet
from ixmp4.data.db.scenario.model import Scenario

from .. import base


class Run(base.BaseModel):
    NotFound: ClassVar = abstract.Run.NotFound
    NotUnique: ClassVar = abstract.Run.NotUnique
    DeletionPrevented: ClassVar = abstract.Run.DeletionPrevented

    NoDefaultVersion: ClassVar = abstract.Run.NoDefaultVersion

    __table_args__ = (db.UniqueConstraint("model__id", "scenario__id", "version"),)

    model__id: types.Integer = db.Column(
        db.ForeignKey("model.id"), nullable=False, index=True
    )
    model: types.Mapped[Model] = db.relationship(
        "Model",
        backref="run",
        foreign_keys=[model__id],
    )

    scenario__id: types.Integer = db.Column(
        db.ForeignKey("scenario.id"),
        nullable=False,
        index=True,
    )
    scenario: types.Mapped[Scenario] = db.relationship(
        "Scenario",
        backref="run",
        foreign_keys=[scenario__id],
    )

    indexset: types.Mapped[list["IndexSet"]] = db.relationship()

    version: types.Integer = db.Column(db.Integer, nullable=False)
    is_default: types.Boolean = db.Column(db.Boolean, default=False, nullable=False)
