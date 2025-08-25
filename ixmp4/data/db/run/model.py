from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db.model.model import Model
from ixmp4.data.db.optimization.equation import Equation
from ixmp4.data.db.optimization.indexset import IndexSet
from ixmp4.data.db.optimization.parameter import Parameter
from ixmp4.data.db.optimization.scalar import Scalar
from ixmp4.data.db.optimization.table import Table
from ixmp4.data.db.optimization.variable import Variable as OptimizationVariable
from ixmp4.data.db.scenario.model import Scenario

from .. import base, mixins, versions


class Run(base.BaseModel, mixins.HasUpdateInfo):
    __tablename__ = "run"
    IsLocked: ClassVar = abstract.Run.IsLocked
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

    equations: types.Mapped[list["Equation"]] = db.relationship()
    indexsets: types.Mapped[list["IndexSet"]] = db.relationship()
    parameters: types.Mapped[list["Parameter"]] = db.relationship()
    scalars: types.Mapped[list["Scalar"]] = db.relationship()
    tables: types.Mapped[list["Table"]] = db.relationship()
    variables: types.Mapped[list["OptimizationVariable"]] = db.relationship()

    version: types.Integer = db.Column(db.Integer, nullable=False)
    is_default: types.Boolean = db.Column(db.Boolean, default=False, nullable=False)
    lock_transaction: types.Mapped[int | None] = db.Column(nullable=True, index=True)


class RunVersion(versions.DefaultVersionModel):
    __tablename__ = "run_version"
    model__id: types.Integer = db.Column(nullable=False, index=True)
    scenario__id: types.Integer = db.Column(nullable=False, index=True)
    version: types.Integer = db.Column(nullable=False)
    is_default: types.Boolean = db.Column(nullable=False)
    lock_transaction: types.Integer = db.Column(nullable=True, index=True)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username
    updated_at: types.DateTime = db.Column(nullable=True)
    updated_by: types.Username


version_triggers = versions.PostgresVersionTriggers(Run.__table__, RunVersion.__table__)
