from typing import Mapping

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.db.iamc.measurand import Measurand
from ixmp4.data.db.region import Region
from ixmp4.data.db.timeseries import TimeSeries as BaseTimeSeries

from .. import base


class TimeSeries(BaseTimeSeries, base.BaseModel):
    __table_args__ = (db.UniqueConstraint("run__id", "region__id", "measurand__id"),)

    region__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("region.id"), nullable=False, index=True
    )
    region: types.Mapped[Region] = db.relationship(
        "Region", backref="metadata", foreign_keys=[region__id], lazy="select"
    )

    measurand__id: types.Integer = db.Column(
        db.Integer, db.ForeignKey("iamc_measurand.id"), nullable=False, index=True
    )
    measurand: types.Mapped[Measurand] = db.relationship(
        "Measurand", backref="metadata", foreign_keys=[measurand__id], lazy="select"
    )

    @property
    def parameters(self) -> Mapping:
        return {
            "region": self.region.name,
            "unit": self.measurand.unit.name,
            "variable": self.measurand.variable.name,
        }
