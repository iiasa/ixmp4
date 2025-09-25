import enum

from sqlalchemy import BigInteger, SmallInteger, orm
from toolkit import db

from ixmp4.rewrite.data.base.db import BaseModel


class Operation(int, enum.Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2


class BaseVersionModel(BaseModel):
    __abstract__ = True
    id: db.t.Integer = orm.mapped_column(primary_key=True)
    transaction_id: db.t.Integer = orm.mapped_column(
        BigInteger(), primary_key=True, index=True, nullable=False
    )
    operation_type: db.t.Integer = orm.mapped_column(
        SmallInteger(), index=True, nullable=False
    )
    end_transaction_id: db.t.Integer = orm.mapped_column(
        BigInteger(), nullable=True, index=True
    )
