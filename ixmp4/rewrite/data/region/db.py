from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data.base.db import BaseModel, HasCreationInfo
from ixmp4.rewrite.data.docs.db import docs_model

from .. import versions

if TYPE_CHECKING:
    from ixmp4.rewrite.data.iamc.timeseries.db import TimeSeries


class Region(BaseModel, HasCreationInfo):
    __tablename__ = "region"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)

    timeseries: db.t.Mapped[list["TimeSeries"]] = orm.relationship(viewonly=True)


RegionDocs = docs_model(Region)


class RegionVersion(versions.BaseVersionModel):
    __tablename__ = "region_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    hierarchy: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Region.__table__, RegionVersion.__table__
)
