from typing import Any, ClassVar

from sqlalchemy.orm import Mapped as Mapped
from sqlalchemy.orm import validates

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base


class Table(
    base.BaseModel,
):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    run__id: Mapped[db.RunId]
    columns: types.Mapped[list["Column"]] = db.relationship()
    data: types.JsonDict = db.Column(db.JsonType, nullable=False, default={})

    @validates("data")
    def validate_data(self, key, data: dict[str, Any]):
        return base.OptimizationData.validate_data(
            # TODO Types don't match since we can't import Column in db/base.py and we
            # can't make Column inherit from abstract.opt.Column because "the metaclass
            # of a derived class must be a (non-strict) subclass of the metaclasses of
            # all its bases" -- So what's the elegant solution here?
            key=key,
            data=data,
            columns=self.columns,  # type: ignore
        )

    __table_args__ = (db.UniqueConstraint("name", "run__id"),)
