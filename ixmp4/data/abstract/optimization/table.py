from typing import Any, Iterable, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository


# TODO: standardize docstrings (run/Run/`Run`)
class Column(base.BaseModel, Protocol):
    """Column data model."""

    name: types.String
    """Unique name of the Table."""
    dtype: types.String
    """Type of the Column's data."""

    table: types.Mapped
    """Associated Table."""
    table__id: types.Integer
    """Foreign unique integer id of a Table."""
    indexset: types.Mapped
    """Associated IndexSet."""
    constrained_to_set: types.Integer
    """Foreign unique integer if of an IndexSet."""

    unique: types.Boolean
    """Boolean to determine whether data in this Column must contribute to Uniqueness
    of Keys."""

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Column {self.id} name={self.name}>"


class ColumnRepository(base.Creator, base.Retriever, Protocol):
    docs: DocsRepository

    def create(
        self,
        name: str,
        dtype: str,
        constrained_to_indexset: int,
        unique: bool,
    ) -> Column:
        """Creates a Column.

        Parameters
        ----------
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
        ...


class Table(base.BaseModel, Protocol):
    """Table data model."""

    name: types.String
    """Unique name of the Table."""
    data: types.JsonDict
    """Data stored in the Table."""
    columns: types.Mapped[list[Column]]
    """Data specifying this Table's Columns."""

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Table {self.id} name={self.name}>"


class TableRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(
        self,
        run_id: int,
        name: str,
        data: dict[str, Any],
        indexsets: list[str | None] | None = None,
    ) -> Table:
        """Creates a Table.

        If `data.keys()` correspond to :class:ixmp4.data.abstract.optimization.IndexSet
        names, these names will be used to link these IndexSets to the Columns.
        Otherwise, `indexsets` is checked analogously, so the order of entries of
        `indexsets` is important. If neither condition is met, the Table might contain
        values that the remaining code does not know how to handle.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Table is
            defined.
        name : str
            The unique name of the Table.
        data : dict[str, Any]
            The data stored in the Table.
        indexsets : list[str | None] | None
            Optional list of IndexSet names that define the allowed contents of
            `data.values()`.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Table.NotUnique`:
            If the Table with `name` already exists for the Run with `run_id`.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Table`:
            The created Table.
        """
        ...

    def get(self, run_id: int, name: str) -> Table:
        """Retrieves a Table.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Table is
            defined.
        name : str
            The name of the Table.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Table.NotFound`:
            If the Table with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Table`:
            The retrieved Table.
        """
        ...

    def get_by_id(self, id: int) -> Table:
        """Retrieves a Table by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Table.NotFound`.
            If the Table with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Table`:
            The retrieved Table.
        """
        ...

    def list(self, *, name: str | None = None, **kwargs) -> Iterable[Table]:
        r"""Lists Tables by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Table. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filters.VariableFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Table`]:
            List of Tables.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate Tables by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Table. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filters.VariableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - value
                - `ixmp4.data.abstract.Unit`
                - `ixmp4.data.abstract.Unit`.id
        """
        ...

    def add_column(
        self, name: str, dtype: str, indexset: str, data: dict[str, Any] | None = None
    ) -> Table:
        """Adds a Column to a Table.

        Parameters
        ----------
        name : str
            The name of the Table to which the Column shall be added.
        dtype : str
            The data type of the Column.
        indexset : str
            The name of the IndexSet the Column will be linked to.
        data : dict[str, Any] | None
            The data the Column will hold.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Table.NotFound`.
            If the Table with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Table`:
            The Table with the added Column.
        """
        ...
