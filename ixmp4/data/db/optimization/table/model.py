from typing import Any, ClassVar

from sqlalchemy.orm import Mapped as Mapped
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.core.exceptions import OptimizationDataValidationError
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base, utils


class Table(base.BaseModel):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DataInvalid: ClassVar = OptimizationDataValidationError
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    run__id: types.RunId
    columns: types.Mapped[list["Column"]] = db.relationship()
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    # TODO: should we pass self to validate_data to raise more specific errors?

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        return utils.validate_data(
            host=self,
            data=data,
            columns=self.columns,
        )

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
