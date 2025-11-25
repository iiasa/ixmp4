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


class Parameter(IndexedModel, HasCreationInfo):
    __tablename__ = "opt_par"
    __table_args__ = (sa.UniqueConstraint("name", "run__id"),)

    indexset_associations: orm.Mapped[list["ParameterIndexsetAssociation"]] = (
        orm.relationship(
            back_populates="parameter",
            cascade="all, delete-orphan",
            order_by="ParameterIndexsetAssociation.id",
            passive_deletes=True,
        )
    )


ParameterDocs = docs_model(Parameter)


class ParameterIndexsetAssociation(IndexsetAssociationModel):
    __tablename__ = "opt_par_idx_association"

    parameter__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("opt_par.id"), nullable=False, index=True
    )

    parameter: orm.Mapped["Parameter"] = orm.relationship()


class ParameterVersion(IndexedVersionModel, HasCreationInfo):
    __tablename__ = "opt_par_version"


class ParameterIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_par_idx_association_version"

    parameter__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    Parameter.__table__, ParameterVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    ParameterIndexsetAssociation.__table__,
    ParameterIndexsetAssociationVersion.__table__,
)
