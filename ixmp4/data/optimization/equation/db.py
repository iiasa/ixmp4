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


class Equation(IndexedModel, HasCreationInfo):
    __tablename__ = "opt_equ"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    indexset_associations: orm.Mapped[list["EquationIndexsetAssociation"]] = (
        orm.relationship(
            back_populates="equation",
            cascade="all, delete-orphan",
            order_by="EquationIndexsetAssociation.id",
            passive_deletes=True,
        )
    )


EquationDocs = docs_model(Equation)


class EquationIndexsetAssociation(IndexsetAssociationModel):
    __tablename__ = "opt_equ_idx_association"

    equation__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("opt_equ.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    equation: orm.Mapped["Equation"] = orm.relationship()

    @classmethod
    def get_item_id_column(cls) -> sa.ColumnElement[int]:
        return cls.equation__id


class EquationVersion(IndexedVersionModel):
    __tablename__ = "opt_equ_version"

    created_at: db.t.DateTime = orm.mapped_column(nullable=True)
    created_by: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


class EquationIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_equ_idx_association_version"

    equation__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    Equation.__table__, EquationVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    EquationIndexsetAssociation.__table__, EquationIndexsetAssociationVersion.__table__
)
