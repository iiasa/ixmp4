from ixmp4.data.auth.decorators import guard

from .. import base
from .docs import ColumnDocsRepository
from .model import Column


class ColumnRepository(
    base.Creator[Column],
    base.Retriever[Column],
):
    model_class = Column

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.docs = ColumnDocsRepository(*args, **kwargs)

        from .filter import OptimizationColumnFilter

        self.filter_class = OptimizationColumnFilter

    def add(
        self,
        table_id: int,
        name: str,
        dtype: str,
        constrained_to_indexset: str,
        unique: bool,
    ) -> Column:
        column = Column(
            table__id=table_id,
            name=name,
            dtype=dtype,
            constrained_to_indexset=constrained_to_indexset,
            unique=unique,
        )
        self.session.add(column)
        return column

    @guard("edit")
    def create(
        self,
        table_id: int,
        name: str,
        dtype: str,
        constrained_to_indexset: int,
        unique: bool,
        **kwargs,
    ) -> Column:
        """Creates a Column.

        Parameters
        ----------
        table_id : int
            The unique integer id of the :class:`ixmp4.data.abstract.optimization.Table`
            this Column belongs to.
        name : str
            The unique name of the Column.
        dtype : str
            The pandas-inferred type of the Column's data.
        constrained_to_indexset : int
            The id of an :class:`ixmp4.data.abstract.optimization.IndexSet`, which must
            contain all values used as entries in this Column.
        unique : bool
            A bool to determine whether entries in this Column should be considered for
            evaluating uniqueness of keys. Defaults to True.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Column.NotUnique`:
            If the Column with `name` already exists for the related
            :class:`ixmp4.data.abstract.optimization.Table`.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Column`:
            The created Column.
        """
        return super().create(
            table_id=table_id,
            name=name,
            dtype=dtype,
            constrained_to_indexset=constrained_to_indexset,
            unique=unique,
            **kwargs,
        )
