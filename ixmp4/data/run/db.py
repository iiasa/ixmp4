from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasUpdateInfo
from ixmp4.data.model.db import Model
from ixmp4.data.scenario.db import Scenario

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries


class Run(BaseModel, HasUpdateInfo):
    __tablename__ = "run"
    __table_args__ = (sa.UniqueConstraint("model__id", "scenario__id", "version"),)

    model__id: db.t.Integer = orm.mapped_column(
        sa.ForeignKey("model.id"), nullable=False, index=True
    )
    model: db.t.Mapped[Model] = orm.relationship(
        "Model",
        backref="run",
        foreign_keys=[model__id],
    )

    scenario__id: db.t.Integer = orm.mapped_column(
        sa.ForeignKey("scenario.id"),
        nullable=False,
        index=True,
    )
    scenario: db.t.Mapped[Scenario] = orm.relationship(
        "Scenario",
        backref="run",
        foreign_keys=[scenario__id],
    )
    timeseries: db.t.Mapped[list["TimeSeries"]] = orm.relationship(viewonly=True)

    # equations: db.t.Mapped[list["Equation"]] = orm.relationship()
    # indexsets: db.t.Mapped[list["IndexSet"]] = orm.relationship()
    # parameters: db.t.Mapped[list["Parameter"]] = orm.relationship()
    # scalars: db.t.Mapped[list["Scalar"]] = orm.relationship()
    # tables: db.t.Mapped[list["Table"]] = orm.relationship()
    # variables: db.t.Mapped[list["OptimizationVariable"]] = orm.relationship()

    version: db.t.Integer = orm.mapped_column(sa.Integer, nullable=False)
    is_default: db.t.Boolean = orm.mapped_column(
        sa.Boolean, default=False, nullable=False
    )
    lock_transaction: db.t.Mapped[int | None] = orm.mapped_column(
        nullable=True, index=True
    )


class RunVersion(versions.BaseVersionModel):
    __tablename__ = "run_version"
    model__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    scenario__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    version: db.t.Integer = orm.mapped_column(nullable=False)
    is_default: db.t.Boolean = orm.mapped_column(nullable=False)
    lock_transaction: db.t.Integer = orm.mapped_column(nullable=True, index=True)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)
    updated_at: db.t.DateTime = orm.mapped_column(nullable=True)
    updated_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(Run.__table__, RunVersion.__table__)
