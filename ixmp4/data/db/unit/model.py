from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base, versions


class Unit(base.BaseModel, mixins.HasCreationInfo):
    __tablename__ = "unit"
    NotFound: ClassVar = abstract.Unit.NotFound
    NotUnique: ClassVar = abstract.Unit.NotUnique
    DeletionPrevented: ClassVar = abstract.Unit.DeletionPrevented

    name: types.UniqueName


class UnitVersion(versions.DefaultVersionModel):
    __tablename__ = "unit_version"
    name: types.String = db.Column(db.String(255), nullable=False)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Unit.__table__, UnitVersion.__table__
)
