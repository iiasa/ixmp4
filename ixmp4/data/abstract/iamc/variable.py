from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from ixmp4.data import types

from .. import base
from ..annotations import (
    HasIdFilter,
    HasNameFilter,
    HasRegionFilter,
    HasRunFilter,
    HasUnitFilter,
)
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


class EnumerateKwargs(HasIdFilter, HasNameFilter, total=False):
    region: HasRegionFilter
    unit: HasUnitFilter
    # TODO leaving something like this renders the type hints while typing to say
    # `run: dict[str, bool | ...]`, from which you can immediately tell the allowed
    # types, but won't actually know which strings to put in as the keys. Using the
    # TypedDict, you'd know you have to look up its definition unless you know by heart
    # what you can enter.
    # run: dict[
    #     str,
    #     bool
    #     | int
    #     | Iterable[int]
    #     | Mapping[str, int | str | Iterable[int] | Iterable[str]],
    # ]
    run: HasRunFilter


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

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Variable]:
        r"""Lists variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filter.VariableFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Variable`]:
            List of variables.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.iamc.variable.filter.VariableFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...
