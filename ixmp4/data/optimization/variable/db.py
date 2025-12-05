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


class Variable(IndexedModel, HasCreationInfo):
    __tablename__ = "opt_var"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    indexset_associations: orm.Mapped[list["VariableIndexsetAssociation"]] = (
        orm.relationship(
            back_populates="variable",
            cascade="all, delete-orphan",
            order_by="VariableIndexsetAssociation.id",
            passive_deletes=True,
        )
    )


VariableDocs = docs_model(Variable)


class VariableIndexsetAssociation(IndexsetAssociationModel):
    __tablename__ = "opt_var_idx_association"

    variable__id: db.t.Integer = orm.mapped_column(
        sa.Integer,
        sa.ForeignKey("opt_var.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    variable: orm.Mapped[Variable] = orm.relationship(Variable)


class VariableVersion(IndexedVersionModel, HasCreationInfo):
    __tablename__ = "opt_var_version"


class VariableIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_var_idx_association_version"

    variable__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    @staticmethod
    def join_variable_versions() -> sa.ColumnElement[bool]:
        return sa.and_(
            VariableIndexsetAssociationVersion.variable__id == VariableVersion.id,
            VariableIndexsetAssociationVersion.join_valid_versions(VariableVersion),
        )

    variable: orm.Relationship["VariableVersion"] = orm.relationship(
        VariableVersion,
        primaryjoin=join_variable_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    Variable.__table__, VariableVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    VariableIndexsetAssociation.__table__, VariableIndexsetAssociationVersion.__table__
)
