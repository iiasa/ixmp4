import sqlalchemy as sa
from sqlalchemy import orm
from toolkit.db.types import Integer, String

from ixmp4.data.base.db import BaseModel


class Checkpoint(BaseModel):
    __tablename__ = "checkpoint"
    message: String = orm.mapped_column(sa.String(1023), nullable=False)
    transaction__id: Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("transaction.id"),
        nullable=True,
        index=True,
    )
    run__id: Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
