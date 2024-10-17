from typing import Any, Iterable, Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository
from .column import Column


class Parameter(base.BaseModel, Protocol):
    """Parameter data model."""

    name: types.String
    """Unique name of the Parameter."""
    data: types.JsonDict
    """Data stored in the Parameter."""
    columns: types.Mapped[list[Column]]
    """Data specifying this Parameter's Columns."""

    run__id: types.Integer
    "Foreign unique integer id of a run."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Parameter {self.id} name={self.name}>"


class ParameterRepository(
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
    ) -> Parameter:
        """Creates a Parameter.

        Each column of the Parameter needs to be constrained to an existing
        :class:ixmp4.data.abstract.optimization.IndexSet. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Parameter is
            defined.
        name : str
            The unique name of the Parameter.
        constrained_to_indexsets : list[str]
            List of :class:`ixmp4.data.abstract.optimization.IndexSet` names that define
            the allowed contents of the Parameter's columns.
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Parameter.NotUnique`:
            If the Parameter with `name` already exists for the Run with `run_id`.
        ValueError
            If `column_names` are not unique or not enough names are given.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Parameter`:
            The created Parameter.
        """
        ...

    def get(self, run_id: int, name: str) -> Parameter:
        """Retrieves a Parameter.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Parameter is
            defined.
        name : str
            The name of the Parameter.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Parameter.NotFound`:
            If the Parameter with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Parameter`:
            The retrieved Parameter.
        """
        ...

    def get_by_id(self, id: int) -> Parameter:
        """Retrieves a Parameter by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Parameter.NotFound`.
            If the Parameter with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Parameter`:
            The retrieved Parameter.
        """
        ...

    def list(self, *, name: str | None = None, **kwargs) -> Iterable[Parameter]:
        r"""Lists Parameters by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Parameter. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.parameter.filter.OptimizationParameterFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Parameter`]:
            List of Parameters.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate Parameters by specified criteria.

        Parameters
        ----------
        name : str
            The name of a Parameter. If supplied only one result will be returned.
        # TODO: Update kwargs
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.optimization.parameter.filter.OptimizationParameterFilter`.

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
    def add_data(self, parameter_id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        r"""Adds data to a Parameter.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Parameter's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Parameter.data`, existing values will be
        overwritten.

        Parameters
        ----------
        parameter_id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Parameter`.
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
