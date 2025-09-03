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


class EquationIndexsetAssociation(BaseIndexSetAssociation):
    __tablename__ = "optimization_equationindexsetassociation"

    equation__id: types.EquationId
    equation: types.Mapped["Equation"] = db.relationship(
        back_populates="_equation_indexset_associations"
    )


class Equation(base.RunLinkedBaseModel):
    __tablename__ = "optimization_equation"

    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Equation.NotFound
    NotUnique: ClassVar = abstract.Equation.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Equation.DeletionPrevented

    # run__id: types.RunId
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @db.validates("data")
    def validate_data(self, key: Any, data: dict[str, Any]) -> dict[str, Any]:
        # Only validate data that has more than the mininum required keys
        if not bool(data.keys() - self._required_keys):
            return data
        utils.validate_data(
            host=self,
            data=data,
            indexsets=self._indexsets,
            column_names=self.column_names,
        )
        return data

    _equation_indexset_associations: types.Mapped[list[EquationIndexsetAssociation]] = (
        db.relationship(
            back_populates="equation",
            cascade="all, delete-orphan",
            order_by="EquationIndexsetAssociation.id",
            passive_deletes=True,
        )
    )

    _indexsets: db.AssociationProxy[list["IndexSet"]] = db.association_proxy(
        "_equation_indexset_associations", "indexset"
    )
    _column_names: db.AssociationProxy[list[str | None]] = db.association_proxy(
        "_equation_indexset_associations", "column_name"
    )

    @property
    def indexset_names(self) -> list[str] | None:
        names = [indexset.name for indexset in self._indexsets]
        return names if bool(names) else None

    @property
    def column_names(self) -> list[str] | None:
        names = [name for name in self._column_names if name]
        return names if bool(names) else None

    updateable_columns = ["data"]

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)

    @property
    def _required_keys(self) -> set[str]:
        return {"levels", "marginals"}


class EquationVersion(versions.RunLinkedVersionModel):
    __tablename__ = "optimization_equation_version"

    name: types.String = db.Column(db.String(255), nullable=False)
    run__id: db.MappedColumn[int] = db.Column(db.Integer, nullable=False, index=True)

    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    created_at: types.DateTime = db.Column(nullable=True)
    created_by: types.Username


class EquationIndexsetAssociationVersion(BaseIndexSetAssociationVersion):
    __tablename__ = "optimization_equationindexsetassociation_version"

    equation__id: db.MappedColumn[int] = db.Column(
        db.Integer, nullable=False, index=True
    )


version_triggers = versions.PostgresVersionTriggers(
    Equation.__table__, EquationVersion.__table__
)
data_version_triggers = versions.PostgresVersionTriggers(
    EquationIndexsetAssociation.__table__, EquationIndexsetAssociationVersion.__table__
)
