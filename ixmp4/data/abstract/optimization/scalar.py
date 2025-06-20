from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from ..annotations import HasUnitIdFilter

if TYPE_CHECKING:
    from . import EnumerateKwargs as BaseEnumerateKwargs

    class EnumerateKwargs(BaseEnumerateKwargs, HasUnitIdFilter, total=False): ...


import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from .. import base
from ..docs import DocsRepository
from ..unit import Unit
from .base import BackendBaseRepository


class Scalar(base.BaseModel, Protocol):
    """Scalar data model."""

    name: str
    """Unique name of the Scalar."""
    value: float
    """Value of the Scalar."""
    unit__id: int
    "Foreign unique integer id of a unit."
    unit: Unit
    "Associated unit."
    run__id: int
    "Foreign unique integer id of a run."

    created_at: datetime
    "Creation date/time. TODO"
    created_by: str
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Scalar {self.id} name={self.name}>"


class ScalarRepository(
    BackendBaseRepository[Scalar],
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(self, run_id: int, name: str, value: float, unit_name: str) -> Scalar:
        """Creates a Scalar.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Scalar is
            defined.
        name : str
            The name of the Scalar.
        value : float
            The value of the Scalar.
        unit_name : str
            The name of the :class:`ixmp4.data.abstract.Unit` for which this Scalar is
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

    def delete(self, id: int) -> None:
        """Deletes a Scalar.

        Parameters
        ----------
        id : int
            The unique integer id of the Scalar.

        Raises
        ------
        :class:`ixmp4.data.abstract.Scalar.NotFound`:
            If the Scalar with `id` does not exist.
        :class:`ixmp4.data.abstract.Scalar.DeletionPrevented`:
            If the Scalar with `id` is used in the database, preventing it's deletion.
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

    def list(self, **kwargs: Unpack["EnumerateKwargs"]) -> Iterable[Scalar]:
        r"""Lists Scalars by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.optimization.scalar.filter.OptimizationScalarFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Scalar`]:
            List of Scalars.
        """
        ...

    def tabulate(self, **kwargs: Unpack["EnumerateKwargs"]) -> pd.DataFrame:
        r"""Tabulate Scalars by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
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
