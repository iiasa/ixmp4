from typing import TYPE_CHECKING, Any, ClassVar

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import base, utils

if TYPE_CHECKING:
    from .. import IndexSet


class ParameterIndexsetAssociation(base.RootBaseModel):
    table_prefix = "optimization_"

    parameter__id: types.ParameterId
    parameter: types.Mapped["Parameter"] = db.relationship(
        back_populates="_parameter_indexset_associations"
    )
    indexset__id: types.IndexSetId
    indexset: types.Mapped["IndexSet"] = db.relationship()

    column_name: types.String = db.Column(db.String(255), nullable=True)


class Parameter(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    run__id: types.RunId
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @db.validates("data")
    def validate_data(self, key: Any, data: dict[str, Any]) -> dict[str, Any]:
        if not bool(data):
            return data
        # This case can only arise when all keys have been deleted
        elif not bool(set(data.keys()) - {"values", "units"}):
            return {}
        utils.validate_data(
            host=self,
            data=data,
            indexsets=self._indexsets,
            column_names=self.column_names,
        )
        return data

    _parameter_indexset_associations: types.Mapped[
        list[ParameterIndexsetAssociation]
    ] = db.relationship(
        back_populates="parameter",
        cascade="all, delete-orphan",
        order_by="ParameterIndexsetAssociation.id",
        passive_deletes=True,
    )

    _indexsets: db.AssociationProxy[list["IndexSet"]] = db.association_proxy(
        "_parameter_indexset_associations", "indexset"
    )
    _column_names: db.AssociationProxy[list[str | None]] = db.association_proxy(
        "_parameter_indexset_associations", "column_name"
    )

    @property
    def indexset_names(self) -> list[str]:
        return [indexset.name for indexset in self._indexsets]

    @property
    def column_names(self) -> list[str] | None:
        names = [name for name in self._column_names if name]
        return names if bool(names) else None

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
