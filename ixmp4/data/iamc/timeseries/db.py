from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.decl_api import declared_attr
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel
from ixmp4.data.iamc.datapoint.db import DataPoint

from ..measurand.db import Measurand

if TYPE_CHECKING:
    from ixmp4.data.iamc.variable.db import Variable
    from ixmp4.data.region.db import Region
    from ixmp4.data.run.db import Run
    from ixmp4.data.unit.db import Unit


class TimeSeries(BaseModel):
    __tablename__ = "iamc_timeseries"

    __table_args__ = (sa.UniqueConstraint("run__id", "region__id", "measurand__id"),)

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )
    run: orm.Mapped["Run"] = orm.relationship(
        "Run", foreign_keys=[run__id], lazy="select", viewonly=True
    )

    region__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("region.id"), nullable=False, index=True
    )
    region: orm.Mapped["Region"] = orm.relationship(
        "Region", foreign_keys=[region__id], lazy="select", viewonly=True
    )

    measurand__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("iamc_measurand.id"), nullable=False, index=True
    )
    measurand: orm.Mapped[Measurand] = orm.relationship(
        "Measurand", foreign_keys=[measurand__id], lazy="select"
    )

    variable: orm.Mapped["Variable"] = orm.relationship(
        "ixmp4.data.iamc.variable.db.Variable",
        secondary=Measurand.__table__,
        viewonly=True,
    )
    unit: orm.Mapped["Unit"] = orm.relationship(
        secondary=Measurand.__table__, viewonly=True
    )

    @declared_attr
    def datapoints(cls) -> orm.Relationship[list["DataPoint"]]:
        return orm.relationship(
            "ixmp4.data.iamc.datapoint.db.DataPoint",
            lazy="select",
            viewonly=True,
        )


class TimeSeriesVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_timeseries_version"
    region__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    measurand__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    run__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    TimeSeries.__table__, TimeSeriesVersion.__table__
)
