import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data.base.db import BaseModel


class Checkpoint(BaseModel):
    __tablename__ = "checkpoint"
    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
    transaction__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("transaction.id"),
        nullable=True,
        index=True,
    )
    message: db.t.String = orm.mapped_column(
        sa.String(1023), nullable=False, unique=True
    )
