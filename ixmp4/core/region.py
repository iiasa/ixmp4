from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.backend import Backend
from ixmp4.data.docs.repository import DocsNotFound
from ixmp4.data.region.dto import Region as RegionDto
from ixmp4.data.region.exceptions import (
    RegionDeletionPrevented,
    RegionNotFound,
    RegionNotUnique,
)
from ixmp4.data.region.filter import RegionFilter
from ixmp4.data.region.service import RegionService

from .base import BaseDocsServiceFacade, BaseFacadeObject


class Region(BaseFacadeObject[RegionService, RegionDto]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    DeletionPrevented = RegionDeletionPrevented

    @property
    def id(self) -> int:
        """Unique id."""
        return self.dto.id

    @property
    def name(self) -> str:
        """Region name."""
        return self.dto.name

    @property
    def hierarchy(self) -> str:
        """Region hierarchy."""
        return self.dto.hierarchy

    @property
    def created_at(self) -> datetime | None:
        return self.dto.created_at

    @property
    def created_by(self) -> str | None:
        return self.dto.created_by

    @property
    def docs(self) -> str | None:
        try:
            return self._service.get_docs(self.id).description
        except DocsNotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._service.delete_docs(self.id)
        else:
            self._service.set_docs(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._service.delete_docs(self.id)
        # TODO: silently failing
        except DocsNotFound:
            return None

    def delete(self) -> None:
        """Deletes the region from the database."""
        self._service.delete_by_id(self.dto.id)

    def _get_service(self, backend: Backend) -> RegionService:
        return backend.regions

    def __str__(self) -> str:
        return f"<Region {self.id} name='{self.name}' hierarchy='{self.hierarchy}'>"


class RegionServiceFacade(
    BaseDocsServiceFacade[Region | int | str, Region, RegionService]
):
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

    def create(
        self,
        name: str,
        hierarchy: str,
    ) -> Region:
        dto = self._service.create(name, hierarchy)
        return Region(self._backend, dto)

    def get_by_name(self, name: str) -> Region:
        dto = self._service.get_by_name(name)
        return Region(self._backend, dto)

    def delete(self, ref: Region | int | str) -> None:
        id = self._get_item_id(ref)
        self._service.delete_by_id(id)

    def list(self, **kwargs: Unpack[RegionFilter]) -> list[Region]:
        regions = self._service.list(**kwargs)
        return [Region(self._backend, dto) for dto in regions]

    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> pd.DataFrame:
        return self._service.tabulate(**kwargs)
