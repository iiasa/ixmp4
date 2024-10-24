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

    data_type: types.OptimizationDataType

    _data: types.Mapped[list["IndexSetData"]] = db.relationship(
        back_populates="indexset"
    )

    @db.hybrid_property
    def data(self) -> list[float | int | str]:
        return (
            []
            if self.data_type is None
            else np.array([d.value for d in self._data], dtype=self.data_type).tolist()
        )

    # NOTE For the core layer (setting and retrieving) to work, the property needs a
    # setter method
    @data.inplace.setter
    def _data_setter(self, value: list[float | int | str]) -> None:
        return None

    run__id: types.RunId

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)


class IndexSetData(base.RootBaseModel):
    indexset: types.Mapped["IndexSet"] = db.relationship(back_populates="_data")
    indexset__id: types.IndexSetId
    value: types.String = db.Column(db.String, nullable=False)

    __table_args__ = (db.UniqueConstraint("indexset__id", "value"),)
