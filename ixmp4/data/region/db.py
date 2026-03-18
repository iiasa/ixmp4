from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.types import DateTime, Mapped, String

from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model

from .. import versions

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries


class Region(BaseModel, HasCreationInfo):
    __tablename__ = "region"

    name: String = orm.mapped_column(sa.String(255), nullable=False, unique=True)
    hierarchy: String = orm.mapped_column(sa.String(1023), nullable=False)

    timeseries: Mapped[list["TimeSeries"]] = orm.relationship(viewonly=True)


RegionDocs = docs_model(Region)


class RegionVersion(versions.BaseVersionModel):
    __tablename__ = "region_version"

    name: String = orm.mapped_column(sa.String(255), nullable=False)
    hierarchy: String = orm.mapped_column(sa.String(1023), nullable=False)

    created_at: DateTime = orm.mapped_column(nullable=True)
    created_by: String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Region.__table__, RegionVersion.__table__
)
