import enum
from typing import Any

import sqlalchemy as sa
from sqlalchemy import BigInteger, SmallInteger, orm
from toolkit import db

from ixmp4.data.base.db import BaseModel


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

    @classmethod
    def join_valid_versions(
        cls, foreign_version_class: type["BaseVersionModel"]
    ) -> sa.ColumnElement[Any]:
        return sa.and_(
            orm.remote(foreign_version_class.transaction_id)
            <= orm.foreign(cls.transaction_id),
            orm.remote(foreign_version_class.operation_type) != Operation.DELETE.value,
            sa.or_(
                orm.remote(foreign_version_class.end_transaction_id)
                > orm.foreign(cls.transaction_id),
                orm.remote(foreign_version_class.end_transaction_id) == sa.null(),
            ),
        )
