import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from . import versions
from .base import BaseModel, HasCreationInfo


class Region(BaseModel, HasCreationInfo):
    __tablename__ = "region"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)


class RegionVersion(versions.BaseVersionModel):
    __tablename__ = "region_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Region.__table__, RegionVersion.__table__
)
