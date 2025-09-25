from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel, HasCreationInfo

if TYPE_CHECKING:
    from ixmp4.rewrite.data.run.db import Run


class Scenario(BaseModel, HasCreationInfo):
    __tablename__ = "scenario"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)

    runs: db.t.Mapped["Run"] = orm.relationship()


class ScenarioVersion(versions.BaseVersionModel):
    __tablename__ = "scenario_version"
    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Scenario.__table__, ScenarioVersion.__table__
)
