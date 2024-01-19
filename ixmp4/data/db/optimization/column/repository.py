from ixmp4.data.abstract import optimization as abstract
from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import ColumnDocsRepository
from .model import Column


class ColumnRepository(
    base.Creator[Column],
    base.Retriever[Column],
    abstract.ColumnRepository,
):
    model_class = Column

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ColumnDocsRepository(*args, **kwargs)

        from .filter import OptimizationColumnFilter

        self.filter_class = OptimizationColumnFilter

    def add(
        self,
        name: str,
        dtype: str,
        constrained_to_indexset: str,
        unique: bool,
    ) -> Column:
        column = Column(
            name=name,
            dtype=dtype,
            constrained_to_indexset=constrained_to_indexset,
            unique=unique,
            **self.get_creation_info(),
        )
        self.session.add(column)
        return column

    @guard("edit")
    def create(
        self,
        name: str,
        dtype: str,
        constrained_to_indexset: int,
        unique: bool,
        **kwargs,
    ) -> Column:
        return super().create(
            name=name,
            dtype=dtype,
            constrained_to_indexset=constrained_to_indexset,
            unique=unique,
            **kwargs,
        )
