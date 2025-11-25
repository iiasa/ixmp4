from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel, HasCreationInfo

if TYPE_CHECKING:
    from ixmp4.rewrite.data.iamc.variable.db import Variable
    from ixmp4.rewrite.data.unit.db import Unit


class Measurand(BaseModel, HasCreationInfo):
    __tablename__ = "iamc_measurand"
    __table_args__ = (sa.UniqueConstraint("variable__id", "unit__id"),)

    variable__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("iamc_variable.id"), nullable=False, index=True
    )
    variable: orm.Mapped["Variable"] = orm.relationship(
        "ixmp4.rewrite.data.iamc.variable.db.Variable",
        backref="measurands",
        foreign_keys=[variable__id],
        lazy="select",
    )

    unit__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("unit.id"), nullable=False, index=True
    )
    unit: orm.Mapped["Unit"] = orm.relationship(
        "Unit",
        backref="measurands",
        foreign_keys=[unit__id],
        lazy="select",
    )


class MeasurandVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_measurand_version"
    variable__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    unit__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Measurand.__table__, MeasurandVersion.__table__
)
