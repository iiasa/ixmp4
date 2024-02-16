from datetime import datetime
from typing import ClassVar

from ixmp4.data.abstract import optimization as abstract

from .. import base
from ..docs import Docs, DocsRepository
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

    created_at: datetime | None
    created_by: str | None


class ColumnDocsRepository(DocsRepository):
    model_class = Docs
    prefix = "docs/optimization/columns/"


class ColumnRepository(
    base.Creator[Column],
    abstract.ColumnRepository,
):
    model_class = Column
    prefix = "optimization/columns/"

    def __init__(self, client, *args, **kwargs) -> None:
        super().__init__(client, *args, **kwargs)
        self.docs = ColumnDocsRepository(self.client)

    # TODO: This is not currently in use, only here because the abstract class has this
    # method
    def create(
        self,
        table_id: int,
        name: str,
        dtype: str,
        constrained_to_indexset: int,
        unique: bool,
    ) -> Column:
        return super().create(
            table_id=table_id,
            name=name,
            dtype=dtype,
            constrained_to_indexset=constrained_to_indexset,
            unique=unique,
        )
