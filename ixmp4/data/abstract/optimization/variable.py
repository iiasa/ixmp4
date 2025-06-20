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


class Variable(base.BaseModel, Protocol):
    """Variable data model."""

    name: str
    """Unique name of the Variable."""
    data: dict[str, list[float] | list[int] | list[str]]
    """Data stored in the Variable."""
    indexset_names: list[str] | None
    """List of the names of the IndexSets the Variable is bound to."""
    column_names: list[str] | None
    """List of the Variable's column names, if distinct from the IndexSet names."""

    run__id: int
    "Foreign unique integer id of a run."

    created_at: datetime
    "Creation date/time. TODO"
    created_by: str
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Variable {self.id} name={self.name}>"


class VariableRepository(
    BackendBaseRepository[Variable],
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
        constrained_to_indexsets: str | list[str] | None = None,
        column_names: list[str] | None = None,
    ) -> Variable:
        """Creates a Variable.

        Each column of the Variable needs to be constrained to an existing
        :class:`ixmp4.data.abstract.optimization.IndexSet`. These are specified by name
        and per default, these will be the column names. They can be overwritten by
        specifying `column_names`, which needs to specify a unique name for each column.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Variable is
            defined.
        name : str
            The unique name of the Variable.
        constrained_to_indexsets : str | list[str] | None = None
            One or a List of :class:`ixmp4.data.abstract.optimization.IndexSet` names
            that define the allowed contents of the Variable's columns. If None, no data
            can be added beyond `levels` and `marginals`!
        column_names: list[str] | None = None
            Optional list of names to use as column names. If given, overwrites the
            names inferred from `constrained_to_indexsets`.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Variable.NotUnique`:
            If the Variable with `name` already exists for the Run with `run_id`.
        :class:`ixmp4.core.exceptions.OptimizationItemUsageError`:
            If `column_names` are not unique or not enough names are given.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Variable`:
            The created Variable.
        """
        ...

    def delete(self, id: int) -> None:
        """Deletes a Variable.

        Parameters
        ----------
        id : int
            The unique integer id of the Variable.

        Raises
        ------
        :class:`ixmp4.data.abstract.Variable.NotFound`:
            If the Variable with `id` does not exist.
        :class:`ixmp4.data.abstract.Variable.DeletionPrevented`:
            If the Variable with `id` is used in the database, preventing it's deletion.
        """
        ...

    def get(self, run_id: int, name: str) -> Variable:
        """Retrieves a Variable.

        Parameters
        ----------
        run_id : int
            The id of the :class:`ixmp4.data.abstract.Run` for which this Variable is
            defined.
        name : str
            The name of the Variable.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Variable.NotFound`:
            If the Variable with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Variable`:
            The retrieved Variable.
        """
        ...

    def get_by_id(self, id: int) -> Variable:
        """Retrieves a Variable by its id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.optimization.Variable.NotFound`.
            If the Variable with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.optimization.Variable`:
            The retrieved Variable.
        """
        ...

    def list(self, **kwargs: Unpack["EnumerateKwargs"]) -> Iterable[Variable]:
        r"""Lists Variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter Variables as specified in
            `ixmp4.data.db.optimization.variable.filter.OptimizationVariableFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.optimization.Variable`]:
            List of Variables.
        """
        ...

    def tabulate(self, **kwargs: Unpack["EnumerateKwargs"]) -> pd.DataFrame:
        r"""Tabulate Variables by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter variables as specified in
            `ixmp4.data.db.optimization.variable.filter.OptimizationVariableFilter`.

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

    # TODO Question for Daniel: do variables need to allow adding data manually?
    # TODO Once present, state how to check which IndexSets are linked and which values
    # they permit
    # TODO Can we remove the r-marker?
    def add_data(self, id: int, data: dict[str, Any] | pd.DataFrame) -> None:
        r"""Adds data to a Variable.

        The data will be validated with the linked constrained
        :class:`ixmp4.data.abstract.optimization.IndexSet`s. For that, `data.keys()`
        must correspond to the names of the Variable's columns. Each column can only
        contain values that are in the linked `IndexSet.data`. Each row of entries
        must be unique. No values can be missing, `None`, or `NaN`. If `data.keys()`
        contains names already present in `Variable.data`, existing values will be
        overwritten.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Variable`.
        data : dict[str, Any] | pandas.DataFrame
            The data to be added.

        Raises
        ------
        :class:`ixmp4.core.exceptions.OptimizationItemUsageError`:
            - If values are missing, `None`, or `NaN`
            - If values are not allowed based on constraints to `Indexset`s
            - If rows are not unique

        Returns
        -------
        None
        """
        ...

    def remove_data(
        self, id: int, data: dict[str, Any] | pd.DataFrame | None = None
    ) -> None:
        """Removes data from a Variable.

        Parameters
        ----------
        id : int
            The id of the :class:`ixmp4.data.abstract.optimization.Variable`.
        data : dict[str, Any] | pandas.DataFrame, optional
            The data to be removed. If specified, remove only specific data. This must
            specify all indexed columns. All other keys/columns are ignored. Otherwise,
            remove all data (the default).

        Returns
        -------
        None
        """
        ...
