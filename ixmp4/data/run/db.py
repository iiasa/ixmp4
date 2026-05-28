from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.types import Boolean, DateTime, Integer, Mapped, String

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasUpdateInfo
from ixmp4.data.model.db import Model
from ixmp4.data.scenario.db import Scenario

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries
    from ixmp4.data.meta.db import RunMetaEntry


class Run(BaseModel, HasUpdateInfo):
    __tablename__ = "run"
    __table_args__ = (sa.UniqueConstraint("model__id", "scenario__id", "version"),)

    model__id: Integer = orm.mapped_column(
        sa.ForeignKey("model.id"), nullable=False, index=True
    )
    model: Mapped[Model] = orm.relationship(
        "Model", backref="run", foreign_keys=[model__id], lazy="joined"
    )

    scenario__id: Integer = orm.mapped_column(
        sa.ForeignKey("scenario.id"), nullable=False, index=True
    )
    scenario: Mapped[Scenario] = orm.relationship(
        "Scenario", backref="run", foreign_keys=[scenario__id], lazy="joined"
    )
    timeseries: Mapped[list["TimeSeries"]] = orm.relationship(viewonly=True)
    meta: Mapped[list["RunMetaEntry"]] = orm.relationship(
        "RunMetaEntry",
        back_populates="run",
        foreign_keys="RunMetaEntry.run__id",
        viewonly=True,
    )

    # equations: Mapped[list["Equation"]] = orm.relationship()
    # indexsets: Mapped[list["IndexSet"]] = orm.relationship()
    # parameters: Mapped[list["Parameter"]] = orm.relationship()
    # scalars: Mapped[list["Scalar"]] = orm.relationship()
    # tables: Mapped[list["Table"]] = orm.relationship()
    # variables: Mapped[list["OptimizationVariable"]] = orm.relationship()

    version: Integer = orm.mapped_column(sa.Integer, nullable=False)
    is_default: Boolean = orm.mapped_column(sa.Boolean, default=False, nullable=False)
    lock_transaction: Mapped[int | None] = orm.mapped_column(nullable=True, index=True)


class RunVersion(versions.BaseVersionModel):
    __tablename__ = "run_version"
    model__id: Integer = orm.mapped_column(nullable=False, index=True)
    scenario__id: Integer = orm.mapped_column(nullable=False, index=True)
    version: Integer = orm.mapped_column(nullable=False)
    is_default: Boolean = orm.mapped_column(nullable=False)
    lock_transaction: Integer = orm.mapped_column(nullable=True, index=True)

    created_at: DateTime = orm.mapped_column(nullable=True)
    created_by: String = orm.mapped_column(sa.String(255), nullable=True)
    updated_at: DateTime = orm.mapped_column(nullable=True)
    updated_by: String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(Run.__table__, RunVersion.__table__)
