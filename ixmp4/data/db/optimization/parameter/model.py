from typing import Any, ClassVar, cast

from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import IndexSet, base, utils


class ParameterIndexsetAssociation(base.RootBaseModel):
    table_prefix = "optimization_"

    parameter_id: types.ParameterId
    parameter: types.Mapped["Parameter"] = db.relationship(
        back_populates="_parameter_indexset_associations"
    )
    indexset_id: types.IndexSetId
    indexset: types.Mapped[IndexSet] = db.relationship()

    column_name: types.String = db.Column(db.String(255), nullable=True)


class Parameter(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    run__id: types.RunId

    _parameter_indexset_associations: types.Mapped[
        list[ParameterIndexsetAssociation]
    ] = db.relationship(back_populates="parameter", cascade="all, delete-orphan")

    _indexsets: db.AssociationProxy[list[IndexSet]] = db.association_proxy(
        "_parameter_indexset_associations", "indexset"
    )
    _column_names: db.AssociationProxy[list[str | None]] = db.association_proxy(
        "_parameter_indexset_associations", "column_name"
    )

    @property
    def indexsets(self) -> list[str]:
        return [indexset.name for indexset in self._indexsets]

    @property
    def column_names(self) -> list[str] | None:
        return cast(list[str], self._column_names) if any(self._column_names) else None

    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @validates("data")
    def validate_data(self, key: Any, data: dict[str, Any]) -> dict[str, Any]:
        utils.validate_data(host=self, data=data, columns=self._indexsets)
        return data

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
