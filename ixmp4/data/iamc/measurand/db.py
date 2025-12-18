from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.decl_api import declared_attr
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries
    from ixmp4.data.iamc.variable.db import Variable
    from ixmp4.data.unit.db import Unit


class Measurand(BaseModel, HasCreationInfo):
    __tablename__ = "iamc_measurand"
    __table_args__ = (sa.UniqueConstraint("variable__id", "unit__id"),)

    variable__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("iamc_variable.id"), nullable=False, index=True
    )

    @declared_attr
    def variable(cls) -> orm.Relationship["Variable"]:
        return orm.relationship(
            "ixmp4.data.iamc.variable.db.Variable",
            backref="measurands",
            foreign_keys="Measurand.variable__id",
            lazy="select",
        )

    unit__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("unit.id"), nullable=False, index=True
    )

    @declared_attr
    def unit(cls) -> orm.Relationship["Unit"]:
        return orm.relationship(
            "ixmp4.data.unit.db.Unit",
            backref="measurands",
            foreign_keys="Measurand.unit__id",
            lazy="select",
        )

    timeseries: db.t.Mapped[list["TimeSeries"]] = orm.relationship(viewonly=True)


class MeasurandVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_measurand_version"
    variable__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    unit__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)

    @staticmethod
    def join_timeseries_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.timeseries.db import TimeSeriesVersion

        return sa.and_(
            orm.foreign(MeasurandVersion.id)
            == orm.remote(TimeSeriesVersion.measurand__id),
            MeasurandVersion.join_valid_versions(TimeSeriesVersion),
        )

    timeseries: orm.Relationship[list["TimeSeries"]] = orm.relationship(
        "ixmp4.data.iamc.timeseries.db.TimeSeriesVersion",
        primaryjoin=join_timeseries_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    Measurand.__table__, MeasurandVersion.__table__
)
