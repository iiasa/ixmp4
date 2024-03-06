from typing import ClassVar

from sqlalchemy.ext.declarative import declared_attr

from ixmp4 import db
from ixmp4.data import abstract

from .. import base


class DataPoint(base.BaseModel):
    NotFound: ClassVar = abstract.DataPoint.NotFound
    NotUnique: ClassVar = abstract.DataPoint.NotUnique
    DeletionPrevented: ClassVar = abstract.DataPoint.DeletionPrevented

    Type: ClassVar = abstract.DataPoint.Type

    __abstract__ = True

    updateable_columns = ["value"]

    @declared_attr
    def time_series__id(cls):
        return db.Column(
            db.Integer,
            db.ForeignKey("iamc_timeseries.id"),
            nullable=False,
            index=True,
        )

    value = db.Column(db.Float)

    type = db.Column(db.String(255), nullable=False, index=True)

    step_category = db.Column(db.String(1023), index=True)
    step_year = db.Column(db.Integer, index=True)
    step_datetime = db.Column(db.DateTime, index=True)


class UniversalDataPoint(DataPoint):
    @declared_attr.directive
    def __tablename__(cls) -> str:
        return "iamc_datapoint_universal"

    __table_args__ = (
        db.UniqueConstraint("time_series__id", "step_year", "step_category"),
        db.UniqueConstraint("time_series__id", "step_datetime"),
    )


def get_datapoint_model(session) -> type[DataPoint]:
    return UniversalDataPoint
