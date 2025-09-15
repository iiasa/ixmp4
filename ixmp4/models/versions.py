import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from .base import BaseModel


class BaseVersionModel(BaseModel):
    __abstract__ = True
    id: db.t.Integer = orm.mapped_column(primary_key=True)

    transaction_id: db.t.Integer = orm.mapped_column(
        sa.BigInteger(), primary_key=True, index=True, nullable=False
    )
    end_transaction_id: db.t.Integer = orm.mapped_column(
        sa.BigInteger(), nullable=True, index=True
    )

    operation_type: db.t.Integer = orm.mapped_column(
        sa.SmallInteger(), index=True, nullable=False
    )
