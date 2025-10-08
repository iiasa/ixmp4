from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel

from ..measurand.db import Measurand

if TYPE_CHECKING:
    from ixmp4.rewrite.data.iamc.datapoint.db import DataPoint
    from ixmp4.rewrite.data.iamc.variable.db import Variable
    from ixmp4.rewrite.data.region.db import Region
    from ixmp4.rewrite.data.unit.db import Unit


class TimeSeries(BaseModel):
    __tablename__ = "iamc_timeseries"

    __table_args__ = (sa.UniqueConstraint("run__id", "region__id", "measurand__id"),)

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )
    run = orm.relationship("Run", foreign_keys=[run__id], lazy="select")

    region__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("region.id"), nullable=False, index=True
    )
    region: orm.Mapped["Region"] = orm.relationship(
        "Region", foreign_keys=[region__id], lazy="select"
    )

    measurand__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("iamc_measurand.id"), nullable=False, index=True
    )
    measurand: orm.Mapped[Measurand] = orm.relationship(
        "Measurand", foreign_keys=[measurand__id], lazy="select"
    )

    variable: db.t.Mapped["Variable"] = orm.relationship(secondary=Measurand.__table__)
    unit: db.t.Mapped["Unit"] = orm.relationship(secondary=Measurand.__table__)
    datapoints: db.t.Mapped[list["DataPoint"]] = orm.relationship()


class TimeSeriesVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_timeseries_version"
    region__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    measurand__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    run__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    TimeSeries.__table__, TimeSeriesVersion.__table__
)
