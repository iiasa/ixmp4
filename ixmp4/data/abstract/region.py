from datetime import datetime
from typing import Protocol

import pandas as pd

# TODO Import this from typing when dropping Python 3.11
from typing_extensions import Unpack

from . import base
from .annotations import (
    HasHierarchyFilter,
    HasIdFilter,
    HasNameFilter,
    IamcRegionFilter,
)
from .docs import DocsRepository


class Region(base.BaseModel, Protocol):
    """Region data model."""

    name: str
    "Unique name of the region."
    hierarchy: str
    "Region hierarchy."

    created_at: datetime
    "Creation date/time."
    created_by: str
    "Creator."

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"


class EnumerateKwargs(HasHierarchyFilter, HasIdFilter, HasNameFilter, total=False):
    iamc: IamcRegionFilter | bool | None


class RegionRepository(
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
    base.VersionManager,
    Protocol,
):
    docs: DocsRepository

    def create(self, name: str, hierarchy: str) -> Region:
        """Creates a region.

        Parameters
        ----------
        name : str
            The name of the region.
        hierarchy : str
            The hierarchy this region is assigned to.

        Raises
        ------
        :class:`ixmp4.data.abstract.Region.NotUnique`:
            If the region with `name` is not unique.


        Returns
        -------
        :class:`ixmp4.ata.base.iamc.Region`:
            The created region.
        """
        ...

    def delete(self, id: int) -> None:
        """Deletes a region.

        Parameters
        ----------
        id : int
            The unique integer id of the region.

        Raises
        ------
        :class:`ixmp4.data.abstract.Region.NotFound`:
            If the region with `id` does not exist.
        :class:`ixmp4.data.abstract.Region.DeletionPrevented`:
            If the region with `id` is used in the database, preventing it's deletion.
        """
        ...

    def get(self, name: str) -> Region:
        """Retrieves a region.

        Parameters
        ----------
        name : str
            The unique name of the region.

        Raises
        ------
        :class:`ixmp4.data.abstract.Region.NotFound`:
            If the region with `name` does not exist.

        Returns
        -------
        :class:`ixmp4.data.base.iamc.Region`:
            The retrieved region.
        """
        ...

    def get_or_create(self, name: str, hierarchy: str | None = None) -> Region:
        try:
            region = self.get(name)
        except Region.NotFound:
            if hierarchy is None:
                raise TypeError(
                    "Argument `hierarchy` is required if `Region` with `name` does not "
                    "exist."
                )
            return self.create(name, hierarchy)

        if hierarchy is not None and region.hierarchy != hierarchy:
            raise Region.NotUnique(name)
        else:
            return region

    def list(self, **kwargs: Unpack[EnumerateKwargs]) -> list[Region]:
        r"""Lists regions by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.region.filter.RegionFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Region`]:
            List of regions.
        """
        ...

    def tabulate(self, **kwargs: Unpack[EnumerateKwargs]) -> pd.DataFrame:
        r"""Tabulate regions by specified criteria.

        Parameters
        ----------
        \*\*kwargs: any
            Any filter parameters as specified in
            `ixmp4.data.db.region.filter.RegionFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...
