import enum

from sqlalchemy import BigInteger, SmallInteger

from ixmp4 import db
from ixmp4.data import types

from .. import base


class Operation(int, enum.Enum):
    INSERT = 0
    UPDATE = 1
    DELETE = 2


class DefaultVersionModel(base.BaseModel):
    __abstract__ = True
    id: types.Integer = db.Column(primary_key=True)
    transaction_id: types.Integer = db.Column(
        BigInteger(), primary_key=True, index=True, nullable=False
    )
    operation_type: types.Integer = db.Column(
        SmallInteger(), index=True, nullable=False
    )
    end_transaction_id: types.Integer = db.Column(
        BigInteger(), nullable=True, index=True
    )
