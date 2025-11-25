from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model
from ixmp4.data.iamc.measurand.db import Measurand

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries


class Unit(BaseModel, HasCreationInfo):
    __tablename__ = "unit"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False, unique=True)

    timeseries: db.t.Mapped["TimeSeries"] = orm.relationship(
        secondary=Measurand.__table__, viewonly=True
    )


UnitDocs = docs_model(Unit)


class UnitVersion(versions.BaseVersionModel):
    __tablename__ = "unit_version"
    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Unit.__table__, UnitVersion.__table__
)
