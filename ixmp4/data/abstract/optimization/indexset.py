from typing import TYPE_CHECKING, List, Protocol

if TYPE_CHECKING:
    from . import EnumerateKwargs

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository
from .base import BackendBaseRepository


class IndexSet(base.BaseModel, Protocol):
    """IndexSet data model."""

    name: types.String
    """Unique name of the IndexSet."""
    run__id: types.Integer
    """The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
    defined. """

    data: types.OptimizationDataList
    """Unique list of str, int, or float."""

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetRepository(
    BackendBaseRepository[IndexSet],
    base.Creator,
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
        self, id: int, data: float | int | str | List[float] | List[int] | List[str]
    ) -> None:
        """Removes data from an existing IndexSet.

        Parameters
        ----------
        id : int
            The id of the target IndexSet.
        data : float | int | str | List[float] | List[int] | List[str]
            The data to be removed from the IndexSet.

        Returns
        -------
        None:
            Due to compatibility with ixmp.
        """
        ...
