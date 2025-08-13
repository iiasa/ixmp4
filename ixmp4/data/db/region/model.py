from typing import ClassVar

from ixmp4 import db
from ixmp4.data import abstract, types
from ixmp4.data.db import mixins

from .. import base, versions


class Region(base.BaseModel, mixins.HasCreationInfo):
    __tablename__ = "region"
    NotFound: ClassVar = abstract.Region.NotFound
    NotUnique: ClassVar = abstract.Region.NotUnique
    DeletionPrevented: ClassVar = abstract.Region.DeletionPrevented

    name: types.UniqueName
    hierarchy: types.String = db.Column(db.String(1023), nullable=False)


class RegionVersion(versions.DefaultVersionModel):
    __tablename__ = "region_version"
    name: types.String = db.Column(db.String(255), nullable=False)
    hierarchy: types.String = db.Column(db.String(1023), nullable=False)

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


version_triggers = versions.PostgresVersionTriggers(
    Region.__table__, RegionVersion.__table__
)
