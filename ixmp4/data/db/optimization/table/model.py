from typing import TYPE_CHECKING, Any, ClassVar

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.db import versions
from ixmp4.data.db.optimization.associations import (
    BaseIndexSetAssociation,
    BaseIndexSetAssociationVersion,
)

from .. import base, utils

if TYPE_CHECKING:
    from .. import IndexSet


class TableIndexsetAssociation(BaseIndexSetAssociation):
    __tablename__ = "opt_tab_idx_association"

    table__id: types.TableId
    table: types.Mapped["Table"] = db.relationship(
        back_populates="_table_indexset_associations"
    )


class Table(base.BaseModel):
    __tablename__ = "opt_tab"

    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    run__id: types.RunId
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @db.validates("data")
    def validate_data(self, key: Any, data: dict[str, Any]) -> dict[str, Any]:
        if not bool(data):
            return data
        utils.validate_data(
            host=self,
            data=data,
            indexsets=self._indexsets,
            column_names=self.column_names,
            has_extra_columns=False,
        )
        return data

    _table_indexset_associations: types.Mapped[list[TableIndexsetAssociation]] = (
        db.relationship(
            back_populates="table",
            cascade="all, delete-orphan",
            order_by="TableIndexsetAssociation.id",
            passive_deletes=True,
        )
    )

    _indexsets: db.AssociationProxy[list["IndexSet"]] = db.association_proxy(
        "_table_indexset_associations", "indexset"
    )
    _column_names: db.AssociationProxy[list[str | None]] = db.association_proxy(
        "_table_indexset_associations", "column_name"
    )

    @property
    def indexset_names(self) -> list[str]:
        return [indexset.name for indexset in self._indexsets]

    @property
    def column_names(self) -> list[str] | None:
        names = [name for name in self._column_names if name]
        return names if bool(names) else None

    updateable_columns = ["data"]

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class TableVersion(versions.DefaultVersionModel):
    __tablename__ = "opt_tab_version"

    name: types.String = db.Column(db.String(255), nullable=False)
    run__id: db.MappedColumn[int] = db.Column(db.Integer, nullable=False, index=True)

    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


class TableIndexsetAssociationVersion(BaseIndexSetAssociationVersion):
    __tablename__ = "opt_tab_idx_association_version"

    table__id: db.MappedColumn[int] = db.Column(db.Integer, nullable=False, index=True)


version_triggers = versions.PostgresVersionTriggers(
    Table.__table__, TableVersion.__table__
)
data_version_triggers = versions.PostgresVersionTriggers(
    TableIndexsetAssociation.__table__, TableIndexsetAssociationVersion.__table__
)
