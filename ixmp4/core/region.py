from datetime import datetime
from typing import List

import pandas as pd
from typing_extensions import Unpack

from ixmp4.core.docs import DocsDescriptor
from ixmp4.data.backend import Backend
from ixmp4.data.region.dto import Region as RegionDto
from ixmp4.data.region.exceptions import (
    RegionDeletionPrevented,
    RegionNotFound,
    RegionNotUnique,
)
from ixmp4.data.region.filter import (
    FacadeRegionFilter,
    facade_to_data_filter,
)
from ixmp4.data.region.service import RegionService

from .base import BaseDocsServiceFacade, BaseFacadeObject


class Region(BaseFacadeObject[RegionService, RegionDto]):
    Filter = FacadeRegionFilter
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    DeletionPrevented = RegionDeletionPrevented

    docs: DocsDescriptor[RegionService, RegionDto] = DocsDescriptor()
    """Region docs."""

    @property
    def id(self) -> int:
        """Unique id."""
        return self._dto.id

    @property
    def name(self) -> str:
        """Region name."""
        return self._dto.name

    @property
    def hierarchy(self) -> str:
        """Region hierarchy."""
        return self._dto.hierarchy

    @property
    def created_at(self) -> datetime | None:
        return self._dto.created_at

    @property
    def created_by(self) -> str | None:
        return self._dto.created_by

    def delete(self) -> None:
        """Deletes this region."""
        self._service.delete_by_id(self._dto.id)

    def _get_service(self, backend: Backend) -> RegionService:
        return backend.regions

    def __str__(self) -> str:
        return f"<Region name='{self.name}' hierarchy='{self.hierarchy}' id={self.id}>"

    def __repr__(self) -> str:
        return str(self)


class RegionServiceFacade(
    BaseDocsServiceFacade[Region | int | str, Region, RegionService]
):
    """Used to manipulate regions on a platform."""

    def _get_service(self, backend: Backend) -> RegionService:
        return backend.regions

    def _get_item_id(self, ref: Region | int | str) -> int:
        if isinstance(ref, Region):
            return ref.id
        elif isinstance(ref, int):
            return ref
        elif isinstance(ref, str):
            dto = self._service.get_by_name(ref)
            return dto.id
        else:
            raise ValueError(f"Invalid reference to region: {ref}")

    def create(self, name: str, hierarchy: str) -> Region:
        """Creates a region.

        .. code:: python

            platform.regions.create("Region", "Hierarchy")
            #> <Region 1 name='Region' hierarchy='Hierarchy'>

        Parameters
        ----------
        name : str
            The name of the region.
        hierarchy : str
            The hierarchy this region is assigned to.

        Raises
        ------
        :class:`RegionNotUnique`:
            If the region with ``name`` is not unique.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`ixmp4.core.region.Region`:
            The created region.
        """

        dto = self._service.create(name, hierarchy)
        return Region(self._backend, dto)

    def get_by_name(self, name: str) -> Region:
        """Retrieves a region by its name.

        .. code:: python

            platform.regions.get_by_name("Region")
            #> <Region 1 name='Region' hierarchy='Hierarchy'>

        Parameters
        ----------
        name : str
            The unique name of the region.

        Raises
        ------
        :class:`RegionNotFound`:
            If the region with `name` does not exist.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        :class:`ixmp4.core.region.Region`:
            The retrieved region.
        """

        dto = self._service.get_by_name(name)
        return Region(self._backend, dto)

    def delete(self, ref: Region | int | str) -> None:
        """Deletes a region.

        .. code:: python

            platform.regions.delete("Region")

        Parameters
        ----------
        ref : :class:`ixmp4.core.region.Region` | int | str
            Region object, region id or region name.

        Raises
        ------
        :class:`RegionNotFound`:
            If no region matching ``ref`` exists.
        :class:`RegionDeletionPrevented`:
            If the region matching ``ref`` is used in the database,
            preventing its deletion.
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        """

        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def list(self, **kwargs: Unpack[FacadeRegionFilter]) -> List[Region]:
        r"""Lists regions by specified criteria.

        .. code:: python

            platform.regions.list()
            #> [<Region 1 name='Region' hierarchy='Hierarchy'>]

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`RegionFilter`.

        Raises
        ------
        :class:`Unauthorized`:
            If the current user is not authorized to perform this action.

        Returns
        -------
        List[:class:`ixmp4.core.region.Region`]:
            List of regions.
        """

        regions = self._service.list(**facade_to_data_filter(kwargs))
        return [Region(self._backend, dto) for dto in regions]

    def tabulate(self, **kwargs: Unpack[FacadeRegionFilter]) -> pd.DataFrame:
        r"""Tabulates regions by specified criteria.

        .. code:: python

            platform.regions.tabulate()
            #>     name  hierarchy  id                 created_at created_by
            # 0  Region  Hierarchy   1 2026-01-14 15:37:49.996256   @unknown

        Parameters
        ----------
        \*\*kwargs: any
            Filter parameters as specified in :class:`RegionFilter`.

        Returns
        -------
        :class:`pandas.DataFrame`:
            A data frame with the columns:
                - id
                - name
                - hierarchy
        """

        return self._service.tabulate(**facade_to_data_filter(kwargs))
