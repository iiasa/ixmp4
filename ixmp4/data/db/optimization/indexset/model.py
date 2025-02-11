from typing import ClassVar

import numpy as np

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import base


class IndexSet(base.BaseModel):
    NotFound: ClassVar = abstract.IndexSet.NotFound
    NotUnique: ClassVar = abstract.IndexSet.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.IndexSet.DeletionPrevented

    _data_type: types.OptimizationDataType

    _data: types.Mapped[list["IndexSetData"]] = db.relationship(
        back_populates="indexset",
        order_by="IndexSetData.id",
        cascade="all, delete",
        passive_deletes=True,
    )

    @property
    def data(self) -> list[float] | list[int] | list[str]:
        return (
            []
            if self._data_type is None
            else np.array([d.value for d in self._data], dtype=self._data_type).tolist()  # type: ignore[return-value]
        )

    @data.setter
    def data(self, value: list[float] | list[int] | list[str]) -> None:
        return None

    run__id: types.RunId

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class IndexSetData(base.RootBaseModel):
    table_prefix = "optimization_"

    indexset: types.Mapped["IndexSet"] = db.relationship(back_populates="_data")
    indexset__id: types.IndexSet__Id
    value: types.String = db.Column(db.String, nullable=False)

    __table_args__ = (db.UniqueConstraint("indexset__id", "value"),)
