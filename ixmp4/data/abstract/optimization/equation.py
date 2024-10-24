from typing import Any, Iterable, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository
from .column import Column


class Equation(base.BaseModel, Protocol):
    """Equation data model."""

    name: types.String
    """Unique name of the Equation."""
    data: types.JsonDict
    """Data stored in the Equation."""
    columns: types.Mapped[list[Column]]
    """Data specifying this Equation's Columns."""

    run__id: types.Integer
    "Foreign unique integer id of a run."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Equation {self.id} name={self.name}>"


class EquationRepository(
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
    ) -> Equation:
        """Creates an Equation.

        Each column of the Equation needs to be constrained to an existing
        :class:ixmp4.data.abstract.optimization.IndexSet. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Equation is
            defined.
        name : str
            The unique name of the Equation.
        constrained_to_indexsets : list[str]
            List of :class:`ixmp4.data.abstract.optimization.IndexSet` names that define
            the allowed contents of the Equation's columns.
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Equation.NotUnique`:
            If the Equation with `name` already exists for the Run with `run_id`.
        ValueError
            If `column_names` are not unique or not enough names are given.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Equation`:
            The created Equation.
        """
        ...

    def get(self, run_id: int, name: str) -> Equation:
        """Retrieves an Equation.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Equation is
            defined.
        name : str
            The name of the Equation.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Equation.NotFound`:
            If the Equation with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Equation`:
            The retrieved Equation.
        """
        ...

    def get_by_id(self, id: int) -> Equation:
        """Retrieves an Equation by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Equation.NotFound`.
            If the Equation with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Equation`:
            The retrieved Equation.
        """
        ...

    def list(self, *, name: str | None = None, **kwargs) -> Iterable[Equation]:
        r"""Lists Equations by specified criteria.

        Parameters
        ----------
        name : str
            The name of an Equation. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter Equations as specified in
            `ixmp4.data.db.optimization.equation.filter.OptimizationEquationFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Equation`]:
            List of Equations.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate Equations by specified criteria.

        Parameters
        ----------
        name : str
            The name of an Equation. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter variables as specified in
            `ixmp4.data.db.optimization.equation.filter.OptimizationEquationFilter`.

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

    # TODO Question for Daniel: do equations need to allow adding data manually?
    # TODO Once present, state how to check which IndexSets are linked and which values
    # they permit
    def add_data(self, equation_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        r"""Adds data to an Equation.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Equation's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Equation.data`, existing values will be
        overwritten.

        Parameters
        ----------
        equation_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Equation`.
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

    def remove_data(self, equation_id: int) -> None:
        """Removes data from an Equation.

        Parameters
        ----------
        equation_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Equation`.

        Returns
        -------
        None
        """
        ...
