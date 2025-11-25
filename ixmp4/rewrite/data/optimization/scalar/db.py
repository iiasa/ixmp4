from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import BaseModel, HasCreationInfo
from ixmp4.rewrite.data.docs.db import docs_model

if TYPE_CHECKING:
    from ixmp4.rewrite.data.run.db import Run
    from ixmp4.rewrite.data.unit.db import Unit


class Scalar(BaseModel, HasCreationInfo):
    __tablename__ = "opt_sca"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)

    value: db.t.Float = orm.mapped_column(nullable=True)

    unit__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("unit.id"), nullable=False, index=True
    )
    unit: orm.Mapped["Unit"] = orm.relationship(
        "Unit", foreign_keys=[unit__id], lazy="select", viewonly=True
    )

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )
    run: orm.Mapped["Run"] = orm.relationship(
        "Run", foreign_keys=[run__id], lazy="select", viewonly=True
    )


ScalarDocs = docs_model(Scalar)


class ScalarVersion(versions.BaseVersionModel):
    __tablename__ = "opt_sca_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    value: db.t.Float = orm.mapped_column(nullable=True)
    unit__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    run__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


version_triggers = versions.PostgresVersionTriggers(
    Scalar.__table__, ScalarVersion.__table__
)
