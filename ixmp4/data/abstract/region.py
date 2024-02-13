from typing import Protocol

import pandas as pd

from ixmp4.data import types

from . import base
from .docs import DocsRepository


class Region(base.BaseModel, Protocol):
    """Region data model."""

    name: types.String
    "Unique name of the region."
    hierarchy: types.String
    "Region hierarchy."

    created_at: types.DateTime
    "Creation date/time. TODO"
    created_by: types.String
    "Creator. TODO"

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"


class RegionRepository(
    base.Creator,
    base.Deleter,
    base.Retriever,
    base.Enumerator,
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

    def delete(self, id: int):
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

    def get_or_create(
        self,
        name: str,
        hierarchy: str | None = None,
    ) -> Region:
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

    def list(
        self,
        *,
        name: str | None = None,
        hierarchy: str | None = None,
        **kwargs,
    ) -> list[Region]:
        r"""Lists regions by specified criteria.

        Parameters
        ----------
        name : str
            The name of a region. If supplied only one result will be returned.
        hierarchy : str
            The hierarchy of a region.
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.region.filters.RegionFilter`.

        Returns
        -------
        Iterable[:class:`ixmp4.data.abstract.Region`]:
            List of regions.
        """
        ...

    def tabulate(
        self,
        *,
        name: str | None = None,
        hierarchy: str | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        r"""Tabulate regions by specified criteria.

        Parameters
        ----------
        name : str
            The name of a region. If supplied only one result will be returned.
        hierarchy : str
            The hierarchy of a region.
        \*\*kwargs: any
            More filter parameters as specified in
            `ixmp4.data.db.region.filters.RegionFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
        """
        ...
