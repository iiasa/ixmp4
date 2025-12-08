import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.data import versions
from ixmp4.data.base.db import HasCreationInfo
from ixmp4.data.docs.db import docs_model
from ixmp4.data.optimization.base.db import (
    IndexedModel,
    IndexedVersionModel,
    IndexsetAssociationModel,
    IndexsetAssociationVersionModel,
)


class Table(IndexedModel["TableIndexsetAssociation"], HasCreationInfo):
    __tablename__ = "opt_tab"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    indexset_associations: orm.Mapped[list["TableIndexsetAssociation"]] = (
        orm.relationship(
            back_populates="table",
            cascade="all, delete-orphan",
            order_by="TableIndexsetAssociation.id",
            passive_deletes=True,
        )
    )


TableDocs = docs_model(Table)


class TableIndexsetAssociation(IndexsetAssociationModel):
    __tablename__ = "opt_tab_idx_association"

    table__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("opt_tab.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    table: orm.Mapped["Table"] = orm.relationship()

    @classmethod
    def get_item_id_column(cls) -> sa.ColumnElement[int]:
        return cls.__table__.c.table__id


class TableVersion(IndexedVersionModel, HasCreationInfo):
    __tablename__ = "opt_tab_version"


class TableIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_tab_idx_association_version"

    table__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    @staticmethod
    def join_table_versions() -> sa.ColumnElement[bool]:
        return sa.and_(
            TableIndexsetAssociationVersion.table__id == TableVersion.id,
            TableIndexsetAssociationVersion.join_valid_versions(TableVersion),
        )

    table: orm.Relationship["TableVersion"] = orm.relationship(
        TableVersion,
        primaryjoin=join_table_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    Table.__table__, TableVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    TableIndexsetAssociation.__table__, TableIndexsetAssociationVersion.__table__
)
