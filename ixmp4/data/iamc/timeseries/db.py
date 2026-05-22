from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.orm.decl_api import declared_attr
from toolkit.db.types import Integer

from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel
from ixmp4.data.iamc.datapoint.db import DataPoint

from ..measurand.db import Measurand

if TYPE_CHECKING:
    from ixmp4.data.iamc.datapoint.db import DataPointVersion
    from ixmp4.data.iamc.measurand.db import MeasurandVersion
    from ixmp4.data.iamc.variable.db import Variable, VariableVersion
    from ixmp4.data.region.db import Region, RegionVersion
    from ixmp4.data.run.db import Run, RunVersion
    from ixmp4.data.unit.db import Unit, UnitVersion


class TimeSeries(BaseModel):
    __tablename__ = "iamc_timeseries"

    __table_args__ = (sa.UniqueConstraint("run__id", "region__id", "measurand__id"),)

    run__id: Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )
    run: orm.Mapped["Run"] = orm.relationship(
        "Run", foreign_keys=[run__id], lazy="select", viewonly=True
    )

    region__id: Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("region.id"), nullable=False, index=True
    )
    region: orm.Mapped["Region"] = orm.relationship(
        "Region", foreign_keys=[region__id], lazy="select", viewonly=True
    )

    measurand__id: Integer = orm.mapped_column(
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
    region__id: Integer = orm.mapped_column(nullable=False, index=True)
    measurand__id: Integer = orm.mapped_column(nullable=False, index=True)
    run__id: Integer = orm.mapped_column(nullable=False, index=True)

    @staticmethod
    def join_run_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.run.db import RunVersion

        return sa.and_(
            orm.foreign(TimeSeriesVersion.run__id) == orm.remote(RunVersion.id),
            TimeSeriesVersion.join_valid_versions(RunVersion),
        )

    run: orm.Relationship["RunVersion"] = orm.relationship(
        "ixmp4.data.run.db.RunVersion",
        primaryjoin=join_run_versions,
        lazy="select",
        viewonly=True,
    )

    @staticmethod
    def join_region_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.region.db import RegionVersion

        return sa.and_(
            orm.foreign(TimeSeriesVersion.region__id) == orm.remote(RegionVersion.id),
            TimeSeriesVersion.join_valid_versions(RegionVersion),
        )

    region: orm.Relationship["RegionVersion"] = orm.relationship(
        "ixmp4.data.region.db.RegionVersion",
        primaryjoin=join_region_versions,
        lazy="select",
        viewonly=True,
    )

    @staticmethod
    def join_measurand_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.measurand.db import MeasurandVersion

        return sa.and_(
            orm.foreign(TimeSeriesVersion.measurand__id)
            == orm.remote(MeasurandVersion.id),
            TimeSeriesVersion.join_valid_versions(MeasurandVersion),
        )

    measurand: orm.Relationship["MeasurandVersion"] = orm.relationship(
        "ixmp4.data.iamc.measurand.db.MeasurandVersion",
        primaryjoin=join_measurand_versions,
        lazy="select",
        viewonly=True,
    )

    @staticmethod
    def join_variable_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.measurand.db import MeasurandVersion
        from ixmp4.data.iamc.variable.db import VariableVersion

        return sa.and_(
            orm.foreign(MeasurandVersion.variable__id)
            == orm.remote(VariableVersion.id),
            MeasurandVersion.join_valid_versions(VariableVersion),
        )

    variable: orm.Relationship["VariableVersion"] = orm.relationship(
        "ixmp4.data.iamc.variable.db.VariableVersion",
        secondary="iamc_measurand_version",
        primaryjoin=join_measurand_versions,
        secondaryjoin=join_variable_versions,
        lazy="select",
        viewonly=True,
        uselist=False,
    )

    @staticmethod
    def join_unit_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.measurand.db import MeasurandVersion
        from ixmp4.data.unit.db import UnitVersion

        return sa.and_(
            orm.foreign(MeasurandVersion.unit__id) == orm.remote(UnitVersion.id),
            MeasurandVersion.join_valid_versions(UnitVersion),
        )

    unit: orm.Relationship["UnitVersion"] = orm.relationship(
        "ixmp4.data.unit.db.UnitVersion",
        secondary="iamc_measurand_version",
        primaryjoin=join_measurand_versions,
        secondaryjoin=join_unit_versions,
        lazy="select",
        viewonly=True,
        uselist=False,
    )

    @staticmethod
    def join_datapoint_versions() -> sa.ColumnElement[bool]:
        from ixmp4.data.iamc.datapoint.db import DataPointVersion

        return sa.and_(
            orm.foreign(TimeSeriesVersion.id)
            == orm.remote(DataPointVersion.time_series__id),
            TimeSeriesVersion.join_valid_versions(DataPointVersion),
        )

    datapoints: orm.Relationship[list["DataPointVersion"]] = orm.relationship(
        "ixmp4.data.iamc.datapoint.db.DataPointVersion",
        primaryjoin=join_datapoint_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(TimeSeries, TimeSeriesVersion)
