from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base, versions


class Scenario(base.BaseModel, mixins.HasCreationInfo):
    __tablename__ = "scenario"
    NotFound: ClassVar = abstract.Scenario.NotFound
    NotUnique: ClassVar = abstract.Scenario.NotUnique
    DeletionPrevented: ClassVar = abstract.Scenario.DeletionPrevented

    name: types.UniqueName


class ScenarioVersion(versions.DefaultVersionModel):
    __tablename__ = "scenario_version"
    name: types.String = db.Column(db.String(255), nullable=False)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Scenario.__table__, ScenarioVersion.__table__
)
