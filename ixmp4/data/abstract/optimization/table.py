from typing import Any, Iterable, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository
from .column import Column


class Table(base.BaseModel, Protocol):
    """Table data model."""

    name: types.String
    """Unique name of the Table."""
    data: types.JsonDict
    """Data stored in the Table."""
    columns: types.Mapped[list[Column]]
    """Data specifying this Table's Columns."""

    run__id: types.Integer
    "Foreign unique integer id of a run."

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
        constrained_to_indexsets: list[str],
        column_names: list[str] | None = None,
    ) -> Table:
        """Creates a Table.

        Each column of the Table needs to be constrained to an existing
        :class:ixmp4.data.abstract.optimization.IndexSet. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Table is
            defined.
        name : str
            The unique name of the Table.
        constrained_to_indexsets : list[str]
            List of :class:`ixmp4.data.abstract.optimization.IndexSet` names that define
            the allowed contents of the Table's columns.
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Table.NotUnique`:
            If the Table with `name` already exists for the Run with `run_id`.
        ValueError
            If `column_names` are not unique or not enough names are given.

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
            `ixmp4.data.db.optimization.table.filter.OptimizationTableFilter`.

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
            `ixmp4.data.db.optimization.table.filter.OptimizationTableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - data
                - run__id
                - created_at
                - created_by
        """
        ...

    # TODO Once present, state how to check which IndexSets are linked and which values
    # they permit
    def add_data(self, table_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        r"""Adds data to a Table.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Table's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Table.data`, existing values will be
        overwritten.

        Parameters
        ----------
        table_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Table`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        ValueError:
            - If values are missing, `None`, or `NaN`
            - If values are not allowed based on constraints to `Indexset`s
            - If rows are not unique

        Returns
        -------
        None
        """
        ...
