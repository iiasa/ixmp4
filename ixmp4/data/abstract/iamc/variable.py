from typing import Protocol

import pandas as pd

from ixmp4.data import types

from .. import base
from ..docs import DocsRepository


class Variable(base.BaseModel, Protocol):
    """Variable data model."""

    name: types.String
    "Unique name of the variable."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(
    base.Creator,
    base.Retriever,
    base.Enumerator,
    Protocol,
):
    docs: DocsRepository

    def create(self, name: str) -> Variable:
        """Creates a variable.

        Parameters
        ----------
        name : str
            The name of the variable.

        Raises
        ------
        :class:`ixmp4.data.abstract.Variable.NotUnique`:
            If the variable with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.data.abstract.Variable`:
            The created variable.
        """
        ...

    def get(self, name: str) -> Variable:
        """Retrieves a variable.

        Parameters
        ----------
        name : str
            The unique name of the variable.

        Raises
        ------
        :class:`ixmp4.data.abstract.Variable.NotFound`:
            If the variable with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Variable`:
            The retrieved variable.
        """
        ...

    def list(self, *, name: str | None = None, **kwargs) -> list[Variable]:
        r"""Lists variables by specified criteria.

        Parameters
        ----------
        name : str
            The name of a variable. If supplied only one result will be returned.
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filters.VariableFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Variable`]:
            List of variables.
        """
        ...

    def tabulate(self, *, name: str | None = None, **kwargs) -> pd.DataFrame:
        r"""Tabulate variables by specified criteria.

        Parameters
        ----------
        name : str
            The name of a variable. If supplied only one result will be returned.
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filters.VariableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...
