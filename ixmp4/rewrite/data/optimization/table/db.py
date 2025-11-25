import sqlalchemy as sa
from sqlalchemy import orm
from toolkit import db

from ixmp4.rewrite.data import versions
from ixmp4.rewrite.data.base.db import HasCreationInfo
from ixmp4.rewrite.data.docs.db import docs_model
from ixmp4.rewrite.data.optimization.base.db import (
    IndexedModel,
    IndexedVersionModel,
    IndexsetAssociationModel,
    IndexsetAssociationVersionModel,
)


class Table(IndexedModel, HasCreationInfo):
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
        sa.Integer, sa.ForeignKey("opt_tab.id"), nullable=False, index=True
    )

    table: orm.Mapped["Table"] = orm.relationship()


class TableVersion(IndexedVersionModel, HasCreationInfo):
    __tablename__ = "opt_tab_version"


class TableIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_tab_idx_association_version"

    table__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    Table.__table__, TableVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    TableIndexsetAssociation.__table__, TableIndexsetAssociationVersion.__table__
)
