from typing import ClassVar

from ixmp4.data.abstract import optimization as abstract

from .. import base
from .indexset import IndexSet


class Column(base.BaseModel):
    NotFound: ClassVar = abstract.Column.NotFound
    NotUnique: ClassVar = abstract.Column.NotUnique
    DeletionPrevented: ClassVar = abstract.Column.DeletionPrevented

    id: int
    name: str
    dtype: str
    table__id: int
    indexset: IndexSet
    constrained_to_indexset: int
    unique: bool
