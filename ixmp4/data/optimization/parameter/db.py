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


class Parameter(IndexedModel["ParameterIndexsetAssociation"], HasCreationInfo):
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
        sa.Integer,
        sa.ForeignKey("opt_par.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    parameter: orm.Mapped["Parameter"] = orm.relationship()

    @classmethod
    def get_item_id_column(cls) -> sa.ColumnElement[int]:
        return cls.__table__.c.parameter__id


class ParameterVersion(IndexedVersionModel, HasCreationInfo):
    __tablename__ = "opt_par_version"


class ParameterIndexsetAssociationVersion(IndexsetAssociationVersionModel):
    __tablename__ = "opt_par_idx_association_version"

    parameter__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)

    @staticmethod
    def join_parameter_versions() -> sa.ColumnElement[bool]:
        return sa.and_(
            ParameterIndexsetAssociationVersion.parameter__id == ParameterVersion.id,
            ParameterIndexsetAssociationVersion.join_valid_versions(ParameterVersion),
        )

    parameter: orm.Relationship["ParameterVersion"] = orm.relationship(
        ParameterVersion,
        primaryjoin=join_parameter_versions,
        lazy="select",
        viewonly=True,
    )


version_triggers = versions.PostgresVersionTriggers(
    Parameter.__table__, ParameterVersion.__table__
)


association_version_triggers = versions.PostgresVersionTriggers(
    ParameterIndexsetAssociation.__table__,
    ParameterIndexsetAssociationVersion.__table__,
)
