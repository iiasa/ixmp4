from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from . import EnumerateKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from .. import base
from ..docs import DocsRepository
from .base import BackendBaseRepository


class Table(base.BaseModel, Protocol):
    """Table data model."""

    name: str
    """Unique name of the Table."""
    data: dict[str, list[float] | list[int] | list[str]]
    """Data stored in the Table."""
    indexset_names: list[str]
    """List of the names of the IndexSets the Table is bound to."""
    column_names: list[str] | None
    """List of the Table's column names, if distinct from the IndexSet names."""

    run__id: int
    "Foreign unique integer id of a run."

    created_at: datetime
    "Creation date/time. TODO"
    created_by: str
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Table {self.id} name={self.name}>"


class TableRepository(
    BackendBaseRepository[Table],
    base.Creator,
    base.Deleter,
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
        :class:`ixmp4.data.abstract.optimization.IndexSet`. These are specified by name
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
        :class:`ixmp4.core.exceptions.OptimizationItemUsageError`:
            If `column_names` are not unique or not enough names are given.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Table`:
            The created Table.
        """
        ...

    def delete(self, id: int) -> None:
        """Deletes a Table.

        Parameters
        ----------
        id : int
            The unique integer id of the Table.

        Raises
        ------
        :class:`ixmp4.data.abstract.Table.NotFound`:
            If the Table with `id` does not exist.
        :class:`ixmp4.data.abstract.Table.DeletionPrevented`:
            If the Table with `id` is used in the database, preventing it's deletion.
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

    def list(self, **kwargs: Unpack["EnumerateKwargs"]) -> Iterable[Table]:
        r"""Lists Tables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.optimization.table.filter.OptimizationTableFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Table`]:
            List of Tables.
        """
        ...

    def tabulate(self, **kwargs: Unpack["EnumerateKwargs"]) -> pd.DataFrame:
        r"""Tabulate Tables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
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
    def add_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
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
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Table`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`ixmp4.core.exceptions.OptimizationDataValidationError`:
            - If values are missing, `None`, or `NaN`
            - If values are not allowed based on constraints to `Indexset`s
            - If rows are not unique

        Returns
        -------
        None
        """
        ...

    def remove_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        r"""Removes data from a Table.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Table`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be removed. This must specify all indexed columns. All other
            keys/columns are ignored.

        Returns
        -------
        None
        """
        ...
