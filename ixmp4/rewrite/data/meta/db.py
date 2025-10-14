from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

if TYPE_CHECKING:
    from ixmp4.rewrite.data.run.db import Run

# TODO Import this from typing when dropping Python 3.11
from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel

from .dto import MetaValueType
from .type import Type


class RunMetaEntry(BaseModel):
    __tablename__ = "runmetaentry"

    __table_args__ = (
        sa.UniqueConstraint(
            "run__id",
            "key",
        ),
    )
    updateable_columns = [
        "dtype",
        "value_int",
        "value_str",
        "value_float",
        "value_bool",
    ]

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("run.id"),
        nullable=False,
        index=True,
    )
    run: db.t.Mapped["Run"] = orm.relationship(
        "Run",
        backref="meta",
        foreign_keys=[run__id],
    )

    key: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)
    dtype: db.t.String = orm.mapped_column(sa.String(20), nullable=False)

    value_int: db.t.Integer = orm.mapped_column(sa.Integer, nullable=True)
    value_str: db.t.String = orm.mapped_column(sa.String(1023), nullable=True)
    value_float: db.t.Float = orm.mapped_column(sa.Float, nullable=True)
    value_bool: db.t.Boolean = orm.mapped_column(sa.Boolean, nullable=True)

    @property
    def value(self) -> MetaValueType:
        type_ = Type(self.dtype)
        col = Type.column_for_type(type_)
        value: MetaValueType = getattr(self, col)
        return value


class RunMetaEntryVersion(versions.BaseVersionModel):
    __tablename__ = "runmetaentry_version"
    run__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    key: db.t.String = orm.mapped_column(sa.String(1023), nullable=False)
    dtype: db.t.String = orm.mapped_column(sa.String(20), nullable=False)

    value_int: db.t.Integer = orm.mapped_column(sa.Integer, nullable=True)
    value_str: db.t.String = orm.mapped_column(sa.String(1023), nullable=True)
    value_float: db.t.Float = orm.mapped_column(sa.Float, nullable=True)
    value_bool: db.t.Boolean = orm.mapped_column(sa.Boolean, nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    RunMetaEntry.__table__, RunMetaEntryVersion.__table__
)
