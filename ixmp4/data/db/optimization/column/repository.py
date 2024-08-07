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
        name: str,
        constrained_to_indexset: str,
        dtype: str,
        equation_id: int,
        parameter_id: int,
        table_id: int,
        variable_id: int,
        unique: bool,
    ) -> Column:
        column = Column(
            name=name,
            constrained_to_indexset=constrained_to_indexset,
            dtype=dtype,
            equation__id=equation_id,
            parameter__id=parameter_id,
            table__id=table_id,
            variable__id=variable_id,
            unique=unique,
        )
        self.session.add(column)
        return column

    @guard("edit")
    def create(
        self,
        name: str,
        constrained_to_indexset: int,
        dtype: str,
        equation_id: int | None = None,
        parameter_id: int | None = None,
        table_id: int | None = None,
        variable_id: int | None = None,
        unique: bool = True,
        **kwargs,
    ) -> Column:
        """Creates a Column.

        Parameters
        ----------
        name : str
            The unique name of the Column.
        constrained_to_indexset : int
            The id of an :class:`ixmp4.data.abstract.optimization.IndexSet`, which must
            contain all values used as entries in this Column.
        dtype : str
            The pandas-inferred type of the Column's data.
        equation_id : int
            The unique integer id of the
            :class:`ixmp4.data.abstract.optimization.Equation` this Column belongs to.
        parameter_id : int | None, default None
            The unique integer id of the
            :class:`ixmp4.data.abstract.optimization.Parameter` this Column belongs to,
            if it belongs to a `Paremeter`.
        table_id : int | None, default None
            The unique integer id of the :class:`ixmp4.data.abstract.optimization.Table`
            this Column belongs to, if it belongs to a `Table`.
        variable_id : int | None, default None
            The unique integer id of the
            :class:`ixmp4.data.abstract.optimization.Variable` this Column belongs to,
            if it belongs to a `Variable`.
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
            name=name,
            constrained_to_indexset=constrained_to_indexset,
            dtype=dtype,
            equation_id=equation_id,
            parameter_id=parameter_id,
            table_id=table_id,
            variable_id=variable_id,
            unique=unique,
            **kwargs,
        )
