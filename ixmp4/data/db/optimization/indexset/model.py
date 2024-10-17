from typing import ClassVar, Literal

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import base


# TODO Feels like there ought to be this kind of functionality already
def cast_data_as_type(
    data: "IndexSetData", type: Literal["float", "int", "str"] | None
) -> float | int | str:
    if type == "str":
        return data.value
    elif type == "int":
        return int(data.value)
    elif type == "float":
        return float(data.value)
    else:  # type is None
        return 0


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
        return [cast_data_as_type(data, self.data_type) for data in self._data]

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
