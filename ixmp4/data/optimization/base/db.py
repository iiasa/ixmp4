from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm.decl_api import declared_attr
from toolkit import db

from ixmp4.data.base.db import BaseModel
from ixmp4.data.versions import BaseVersionModel

if TYPE_CHECKING:
    from ixmp4.data.optimization.indexset.db import IndexSet
    from ixmp4.data.run.db import Run


class IndexsetAssociationModel(BaseModel):
    __abstract__ = True

    indexset__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("opt_idx.id"), nullable=False, index=True
    )
    column_name: db.t.String = orm.mapped_column(sa.String(255), nullable=True)

    @declared_attr
    def indexset(cls) -> orm.Relationship["IndexSet"]:
        return orm.relationship(
            "IndexSet", foreign_keys=[cls.indexset__id], lazy="select", viewonly=True
        )

    @classmethod
    def get_item_id_column(cls) -> sa.ColumnElement[int]:
        raise NotImplementedError


class IndexsetAssociationVersionModel(BaseVersionModel):
    __abstract__ = True

    indexset__id: db.t.Integer = orm.mapped_column(nullable=False, index=True)
    column_name: db.t.String = orm.mapped_column(sa.String(255), nullable=True)


class IndexedModel(BaseModel):
    __abstract__ = True

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)

    run__id: db.t.Integer = orm.mapped_column(
        sa.Integer, sa.ForeignKey("run.id"), nullable=False, index=True
    )

    @declared_attr
    def run(cls) -> orm.Relationship["Run"]:
        return orm.relationship(
            "Run", foreign_keys=[cls.run__id], lazy="select", viewonly=True
        )

    data: db.t.Mapped[dict[str, list[float] | list[int] | list[str]]] = (
        orm.mapped_column(  # TODO: get rid of this sensibly
            sa.JSON().with_variant(JSONB(), "postgresql"), nullable=False, default={}
        )
    )

    indexset_associations: orm.Relationship[list["IndexsetAssociationModel"]]
    indexsets: AssociationProxy[list["IndexSet"]] = association_proxy(
        "indexset_associations", "indexset"
    )

    @property
    def indexset_names(self) -> list[str] | None:
        names = [indexset.name for indexset in self.indexsets]
        return names if bool(names) else None

    @property
    def column_names(self) -> list[str] | None:
        names = [a.column_name for a in self.indexset_associations if a.column_name]
        return names if bool(names) else None


class IndexedVersionModel(BaseVersionModel):
    __abstract__ = True

    name: db.t.String = orm.mapped_column(sa.String(255), nullable=False)
    run__id: db.t.Integer = orm.mapped_column(sa.Integer, nullable=False, index=True)

    data: db.t.Mapped[dict[str, list[float] | list[int] | list[str]]] = (
        orm.mapped_column(  # TODO: get rid of this sensibly
            sa.JSON().with_variant(JSONB(), "postgresql"), nullable=False, default={}
        )
    )
