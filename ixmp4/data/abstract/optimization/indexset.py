from datetime import datetime
from typing import TYPE_CHECKING, List, Protocol

if TYPE_CHECKING:
    from . import EnumerateKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from .. import base
from ..docs import DocsRepository
from .base import BackendBaseRepository


class IndexSet(base.BaseModel, Protocol):
    """IndexSet data model."""

    name: str
    """Unique name of the IndexSet."""
    run__id: int
    """The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
    defined. """

    data: list[float] | list[int] | list[str]
    """Unique list of str, int, or float."""

    created_at: datetime
    "Creation date/time. TODO"
    created_by: str
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetRepository(
    BackendBaseRepository[IndexSet],
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(self, run_id: int, name: str) -> IndexSet:
        """Creates an IndexSet.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
            defined.
        name : str
            The name of the IndexSet.

        Raises
        ------
        :class:`ixmp4.data.abstract.IndexSet.NotUnique`:
            If the IndexSet with `name` already exists.

        Returns
        -------
        :class:`ixmp4.data.abstract.IndexSet`:
            The created IndexSet.
        """
        ...

    def delete(self, id: int) -> None:
        """Deletes an IndexSet.

        Parameters
        ----------
        id : int
            The unique integer id of the IndexSet.

        Raises
        ------
        :class:`ixmp4.data.abstract.IndexSet.NotFound`:
            If the IndexSet with `id` does not exist.
        :class:`ixmp4.data.abstract.IndexSet.DeletionPrevented`:
            If the IndexSet with `id` is used in the database, preventing it's deletion.
        """
        ...

    def get(self, run_id: int, name: str) -> IndexSet:
        """Retrieves an IndexSet.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
            defined.
        name : str
            The unique name of the IndexSet.

        Raises
        ------
        :class:`ixmp4.data.abstract.IndexSet.NotFound`:
            If the IndexSet with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.IndexSet`:
            The retrieved IndexSet.
        """
        ...

    def list(self, **kwargs: Unpack["EnumerateKwargs"]) -> list[IndexSet]:
        r"""Lists IndexSets by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.optimization.indexset.filter.OptimizationIndexSetFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.IndexSet`]:
            List of IndexSets.
        """
        ...

    def tabulate(self, **kwargs: Unpack["EnumerateKwargs"]) -> pd.DataFrame:
        r"""Tabulate IndexSets by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.optimization.indexset.filter.OptimizationIndexSetFilter`.

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

    def add_data(
        self, id: int, data: float | int | str | List[float] | List[int] | List[str]
    ) -> None:
        """Adds data to an existing IndexSet.

        Parameters
        ----------
        id : int
            The id of the target IndexSet.
        data : float | int | str | List[float] | List[int] | List[str]
            The data to be added to the IndexSet.

        Returns
        -------
        None:
            Due to compatibility with ixmp.
        """
        ...

    def remove_data(
        self,
        id: int,
        data: float | int | str | List[float] | List[int] | List[str],
        remove_dependent_data: bool = True,
    ) -> None:
        """Removes data from an existing IndexSet.

        Parameters
        ----------
        id : int
            The id of the target IndexSet.
        data : float | int | str | List[float] | List[int] | List[str]
            The data to be removed from the IndexSet.
        remove_dependent_data : bool, optional
            Whether to delete data from all linked items referencing `data`.
            Default: `True`.

        Returns
        -------
        None:
            Due to compatibility with ixmp.
        """
        ...
