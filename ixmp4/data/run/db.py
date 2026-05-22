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
    from ixmp4.data.model.db import ModelVersion
    from ixmp4.data.scenario.db import ScenarioVersion


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

    @staticmethod
    def join_model_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.model.db import ModelVersion

        return sa.and_(
            orm.foreign(RunVersion.model__id) == orm.remote(ModelVersion.id),
            RunVersion.join_valid_versions(ModelVersion),
        )

    model: orm.Relationship["ModelVersion"] = orm.relationship(
        "ixmp4.data.model.db.ModelVersion",
        primaryjoin=join_model_versions,
        lazy="select",
        viewonly=True,
    )

    @staticmethod
    def join_scenario_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.scenario.db import ScenarioVersion

        return sa.and_(
            orm.foreign(RunVersion.scenario__id) == orm.remote(ScenarioVersion.id),
            RunVersion.join_valid_versions(ScenarioVersion),
        )

    scenario: orm.Relationship["ScenarioVersion"] = orm.relationship(
        "ixmp4.data.scenario.db.ScenarioVersion",
        primaryjoin=join_scenario_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(Run, RunVersion)
