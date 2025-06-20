from datetime import datetime
from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from . import base
from .annotations import HasIdFilter, HasNameFilter, IamcUnitFilter
from .docs import DocsRepository


class Unit(base.BaseModel, Protocol):
    """Unit data model."""

    name: str
    "Unique name of the unit."
    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Unit {self.id} name={self.name}>"


class EnumerateKwargs(HasIdFilter, HasNameFilter, total=False):
    iamc: IamcUnitFilter | bool


class UnitRepository(
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
    base.VersionManager,
    Protocol,
):
    docs: DocsRepository

    def create(self, name: str) -> Unit:
        """Creates a unit.

        Parameters
        ----------
        name : str
            The name of the model.

        Raises
        ------
        :class:`ixmp4.core.exceptions.UnitNotUnique`:
            If the unit with `name` is not unique.

        Returns
        -------
        :class:`ixmp4.data.abstract.Unit`:
            The created unit.
        """
        ...

    def get(self, name: str) -> Unit:
        """Retrieves a unit.

        Parameters
        ----------
        name : str
            The unique name of the unit.

        Raises
        ------
        :class:`ixmp4.data.abstract.Unit.NotFound`:
            If the unit with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Unit`:
            The retrieved unit.
        """
        ...

    def get_by_id(self, id: int) -> Unit:
        """Retrieves a Unit by it's id.

        Parameters
        ----------
        id : int
            Unique integer id.

        Raises
        ------
        :class:`ixmp4.data.abstract.Unit.NotFound`.
            If the Unit with `id` does not exist.

        Returns
        -------
        :class:`ixmp4.data.abstract.Unit`:
            The retrieved Unit.
        """
        ...

    def get_or_create(self, name: str) -> Unit:
        try:
            return self.get(name)
        except Unit.NotFound:
            return self.create(name)

    def delete(self, id: int) -> None:
        """Deletes a unit.

        Parameters
        ----------
        id : int
            The unique integer id of the unit.

        Raises
        ------
        :class:`ixmp4.data.abstract.Unit.NotFound`:
            If the unit with `id` does not exist.
        :class:`ixmp4.data.abstract.Unit.DeletionPrevented`:
            If the unit with `id` is used in the database, preventing it's deletion.
        """
        ...

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Unit]:
        r"""Lists units by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.unit.filter.UnitFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.base.iamc.Unit`]:
            List of units.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate units by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.unit.filter.UnitFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:pass
                - id
                - name
        """
        ...
