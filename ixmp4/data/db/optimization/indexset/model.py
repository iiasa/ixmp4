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
        back_populates="indexset"
    )

    @property
    def data(self) -> list[float | int | str]:
        return (
            []
            if self._data_type is None
            else np.array([d.value for d in self._data], dtype=self._data_type).tolist()
        )

    @data.setter
    def data(self, value: list[float | int | str]) -> None:
        return None

    run__id: types.RunId

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class IndexSetData(base.RootBaseModel):
    table_prefix = "optimization_"

    indexset: types.Mapped["IndexSet"] = db.relationship(back_populates="_data")
    indexset__id: types.IndexSetId
    value: types.String = db.Column(db.String, nullable=False)

    __table_args__ = (db.UniqueConstraint("indexset__id", "value"),)
