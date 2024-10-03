import copy
from typing import Any, ClassVar

from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base, utils


class Parameter(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Parameter.NotFound
    NotUnique: ClassVar = abstract.Parameter.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Parameter.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    run__id: types.RunId
    columns: types.Mapped[list["Column"]] = db.relationship()
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        data_to_validate = copy.deepcopy(data)
        del data_to_validate["values"]
        del data_to_validate["units"]
        _ = utils.validate_data(
            host=self,
            data=data_to_validate,
            columns=self.columns,
        )
        return data

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
