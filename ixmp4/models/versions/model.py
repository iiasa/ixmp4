import enum

from sqlalchemy import BigInteger, SmallInteger, orm

from ixmp4.data import types

from .. import base


class Operation(int, enum.Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2


class BaseVersionModel(base.BaseModel):
    __abstract__ = True
    id: types.Integer = orm.mapped_column(primary_key=True)
    transaction_id: types.Integer = orm.mapped_column(
        BigInteger(), primary_key=True, index=True, nullable=False
    )
    operation_type: types.Integer = orm.mapped_column(
        SmallInteger(), index=True, nullable=False
    )
    end_transaction_id: types.Integer = orm.mapped_column(
        BigInteger(), nullable=True, index=True
    )
