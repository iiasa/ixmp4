from typing import List, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository


class IndexSet(base.BaseModel, Protocol):
    """IndexSet data model."""

    name: types.String
    """Unique name of the IndexSet."""
    run__id: types.Integer
    """The id of the :class:`ixmp4.data.abstract.Run` for which this IndexSet is
    defined. """

    elements: types.JsonList
    """Unique list of str or int."""

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<IndexSet {self.id} name={self.name}>"


class IndexSetRepository(
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

    def list(self, *, name: str | None = None, **kwargs) -> list[IndexSet]:
        r"""Lists IndexSets by specified criteria.

        Parameters
        ----------
        name : str
            The name of an IndexSet. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.indexset.filter.OptimizationIndexSetFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.IndexSet`]:
            List of IndexSets.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate IndexSets by specified criteria.

        Parameters
        ----------
        name : str
            The name of an IndexSet. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.indexset.filter.OptimizationIndexSetFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - elements
                - run__id
                - created_at
                - created_by
        """
        ...

    def add_elements(
        self, indexset_id: int, elements: float | int | List[float | int | str] | str
    ) -> None:
        """Adds elements to an existing IndexSet.

        Parameters
        ----------
        indexset_id : int
            The id of the target IndexSet.
        elements : float | int | List[float | int | str] | str
            The elements to be added to the IndexSet.

        Returns
        -------
        None:
            Due to compatibility with ixmp.
        """
        ...
