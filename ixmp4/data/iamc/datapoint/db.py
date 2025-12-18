from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel

if TYPE_CHECKING:
    from ixmp4.data.iamc.timeseries.db import TimeSeries


class DataPoint(BaseModel):
    __tablename__ = "iamc_datapoint_universal"

    __table_args__ = (
        sa.UniqueConstraint("time_series__id", "step_year", "step_category"),
        sa.UniqueConstraint("time_series__id", "step_datetime"),
        # sa.CheckConstraint("(step_datetime IS NOT NULL) OR (step_year IS NOT NULL)"),
    )

    time_series__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("iamc_timeseries.id"),
        nullable=False,
        index=True,
    )
    timeseries: db.t.Mapped["TimeSeries"] = orm.relationship(viewonly=True)

    # TODO: Should this column really be nullable?
    value: db.t.Float = orm.mapped_column(nullable=True)

    type: db.t.String = orm.mapped_column(sa.String(255), nullable=False, index=True)

    step_category: db.t.String = orm.mapped_column(
        sa.String(1023), index=True, nullable=True
    )
    step_year: db.t.Integer = orm.mapped_column(index=True, nullable=True)
    step_datetime: db.t.DateTime = orm.mapped_column(index=True, nullable=True)


class DataPointVersion(versions.BaseVersionModel):
    __tablename__ = "iamc_datapoint_universal_version"

    value: db.t.Float = orm.mapped_column(nullable=True)
    type: db.t.String = orm.mapped_column(sa.String(255), nullable=False, index=True)

    time_series__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        nullable=False,
        index=True,
    )

    step_category: db.t.String = orm.mapped_column(
        sa.String(1023), index=True, nullable=True
    )
    step_year: db.t.Integer = orm.mapped_column(index=True, nullable=True)
    step_datetime: db.t.DateTime = orm.mapped_column(index=True, nullable=True)

    @staticmethod
    def join_timeseries_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.timeseries.db import TimeSeriesVersion

        return sa.and_(
            DataPointVersion.time_series__id == TimeSeriesVersion.id,
            DataPointVersion.join_valid_versions(TimeSeriesVersion),
        )

    timeseries: orm.Relationship["TimeSeries"] = orm.relationship(
        "ixmp4.data.iamc.timeseries.db.TimeSeriesVersion",
        primaryjoin=join_timeseries_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    DataPoint.__table__, DataPointVersion.__table__
)
