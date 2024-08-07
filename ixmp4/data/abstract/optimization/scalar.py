from typing import Iterable, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository


class Scalar(base.BaseModel, Protocol):
    """Scalar data model."""

    name: types.String
    """Unique name of the Scalar."""
    value: types.Float
    """Value of the Scalar."""
    unit__id: types.Integer
    "Foreign unique integer id of a unit."
    unit: types.Mapped
    "Associated unit."
    run__id: types.Integer
    "Foreign unique integer id of a run."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Scalar {self.id} name={self.name}>"


class ScalarRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(self, name: str, value: float, unit_name: str, run_id: int) -> Scalar:
        """Creates a Scalar.

        Parameters
        ----------
        name : str
            The name of the Scalar.
        value : float
            The value of the Scalar.
        unit_name : str
            The name of the :class:`ixmp4.data.abstract.Unit` for which this Scalar is
            defined.
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Scalar is
            defined.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Scalar.NotUnique`:
            If the Scalar with `name` already exists for the Run with `run_id`.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Scalar`:
            The created Scalar.
        """
        ...

    def update(
        self, id: int, value: float | None = None, unit_id: int | None = None
    ) -> Scalar:
        """Updates a Scalar.

        Parameters
        ----------
        id : int
            The integer id of the Scalar.
        value : float, optional
            The value of the Scalar.
        unit_id : int, optional
            The id of the :class:`ixmp4.data.abstract.Unit` for which this Scalar is
            defined.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Scalar`:
            The updated Scalar.
        """
        ...

    def get(self, run_id: int, name: str) -> Scalar:
        """Retrieves a Scalar.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Scalar is
            defined.
        name : str
            The name of the Scalar.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Scalar.NotFound`:
            If the Scalar with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Scalar`:
            The retrieved Scalar.
        """
        ...

    def get_by_id(self, id: int) -> Scalar:
        """Retrieves a Scalar by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Scalar.NotFound`.
            If the Scalar with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Scalar`:
            The retrieved Scalar.
        """
        ...

    def list(self, *, name: str | None = None, **kwargs) -> Iterable[Scalar]:
        r"""Lists Scalars by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Scalar. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.scalar.filter.OptimizationScalarFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Scalar`]:
            List of Scalars.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate Scalars by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Scalar. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.scalar.filter.OptimizationScalarFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - value
                - unit__id
                - run__id
                - created_at
                - created_by
        """
        ...
