from typing import ClassVar

from sqlalchemy.orm import validates

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

    elements: types.JsonList = db.Column(db.JsonType, nullable=False, default=[])

    @validates("elements")
    def validate_elements(self, key, value: list[float | int | str]):
        unique = set()
        for element in value:
            if element in unique:
                raise self.DataInvalid(
                    f"{element} already defined for IndexSet {self.name}!"
                )
            else:
                unique.add(element)
        return value

    run__id: types.RunId

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
