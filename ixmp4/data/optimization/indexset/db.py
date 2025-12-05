import logging
from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.base_exceptions import ProgrammingError
from ixmp4.data import versions
from ixmp4.data.base.db import BaseModel, HasCreationInfo
from ixmp4.data.docs.db import docs_model

from .type import Type

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from ixmp4.data.run.db import Run


class IndexSet(BaseModel, HasCreationInfo):
    __tablename__ = "opt_idx"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    data_entries: orm.Mapped[list["IndexSetData"]] = orm.relationship(
        back_populates="indexset",
        order_by="IndexSetData.id",
        cascade="all, delete",
        passive_deletes=True,
    )

    @property
    def data(self) -> list[str] | list[int] | list[float]:
        if self.data_type is None:
            if len(self.data_entries) != 0:
                logger.error(  # TODO Check if this is happening
                    # and mb reset the datatype
                    "Invalid state: data_type is None, but data entries"
                    "are associated with the IndexSet. "
                )
            return []
        else:
            pytype = Type(self.data_type).to_pytype()
        return [pytype(d.value) for d in self.data_entries]

    @data.setter
    def data(self, value: list[float] | list[int] | list[str]) -> None:
        raise ProgrammingError("Cannot set `IndexSet.data`.")

    data_type: db.t.String = orm.mapped_column(sa.String(63), nullable=True)

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )
    run: orm.Mapped["Run"] = orm.relationship(
        "Run", foreign_keys=[run__id], lazy="select", viewonly=True
    )


IndexSetDocs = docs_model(IndexSet)


class IndexSetData(BaseModel):
    __tablename__ = "opt_idx_data"
    __table_args__ = (sa.UniqueConstraint("indexset__id", "value"),)

    indexset__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("opt_idx.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    indexset: orm.Mapped["IndexSet"] = orm.relationship(back_populates="data_entries")
    value: db.t.String = orm.mapped_column(nullable=False)


class IndexSetVersion(versions.BaseVersionModel):
    __tablename__ = "opt_idx_version"

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    run__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    data_type: orm.Mapped[Type] = orm.mapped_column(sa.String(63), nullable=True)

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


class IndexSetDataVersion(versions.BaseVersionModel):
    __tablename__ = "opt_idx_data_version"

    indexset__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    value: db.t.String = orm.mapped_column(nullable=False)
    indexset: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    @staticmethod
    def join_indexset_versions() -> sa.ColumnElement[bool]:
        return sa.and_(
            IndexSetDataVersion.indexset__id == IndexSetVersion.id,
            IndexSetDataVersion.join_valid_versions(IndexSetVersion),
        )

    indexset: orm.Relationship["IndexSetVersion"] = orm.relationship(
        IndexSetVersion,
        primaryjoin=join_indexset_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    IndexSet.__table__, IndexSetVersion.__table__
)
data_version_triggers = versions.PostgresVersionTriggers(
    IndexSetData.__table__, IndexSetDataVersion.__table__
)
