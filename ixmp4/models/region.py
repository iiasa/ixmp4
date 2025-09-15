import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from .base import BaseModel
from .versions import BaseVersionModel


class Region(BaseModel):
    __tablename__ = "region"
    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)


class RegionVersion(BaseVersionModel):
    __tablename__ = "region_version"
    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)
