from typing import ClassVar

from ixmp4 import db
from ixmp4.data import types
from ixmp4.data.abstract import optimization as abstract

from .. import Column, base


class Table(
    base.BaseModel,
    base.OptimizationDataMixin,
    base.RunIDMixin,
    base.UniqueNameRunIDMixin,
):
    # NOTE: These might be mixin-able, but would require some abstraction
    NotFound: ClassVar = abstract.Table.NotFound
    NotUnique: ClassVar = abstract.Table.NotUnique
    DeletionPrevented: ClassVar = abstract.Table.DeletionPrevented

    # constrained_to_indexsets: ClassVar[list[str] | None] = None

    # TODO Types don't match since we can't import Column in db/base.py and we can't
    # make Column inherit from abstract.opt.Column because "the metaclass of a derived
    # class must be a (non-strict) subclass of the metaclasses of all its bases"
    # So what's the elegant solution here?
    columns: types.Mapped[list["Column"]] = db.relationship()  # type: ignore
