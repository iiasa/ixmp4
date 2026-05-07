from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.types import DateTime, Mapped, String

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model

if TYPE_CHECKING:
    from ixmp4.data.run.db import Run


class Scenario(BaseModel, HasCreationInfo):
    __tablename__ = "scenario"

    name: String = orm.mapped_column(sa.String(255), nullable=False, unique=True)

    runs: Mapped[list["Run"]] = orm.relationship(viewonly=True)


ScenarioDocs = docs_model(Scenario)


class ScenarioVersion(versions.BaseVersionModel):
    __tablename__ = "scenario_version"
    name: String = orm.mapped_column(sa.String(255), nullable=False)

    created_at: DateTime = orm.mapped_column(nullable=True)
    created_by: String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Scenario.__table__, ScenarioVersion.__table__
)
