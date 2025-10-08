from collections.abc import Iterable
from datetime import datetime

import pandas as pd
from typing_extensions import Unpack

from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.rewrite.backend import Backend
from ixmp4.rewrite.data.region.dto import Region as RegionModel
from ixmp4.rewrite.data.region.filter import RegionFilter

from .base import BaseFacade


class Region(BaseFacade):
    dto: RegionModel

    def __init__(self, backend: Backend, dto: RegionModel) -> None:
        super().__init__(backend)
        self.dto = dto

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

    def delete(self) -> None:
        """Deletes the region from the database."""
        self._backend.regions.delete(self.dto.id)

    @property
    def docs(self) -> str | None:
        try:
            return self._backend.regions.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description: str | None) -> None:
        if description is None:
            self._backend.regions.docs.delete(self.id)
        else:
            self._backend.regions.docs.set(self.id, description)

    @docs.deleter
    def docs(self) -> None:
        try:
            self._backend.regions.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"


class RegionRepository(BaseFacade):
    def _get_region_id(self, region: str | int | Region | None) -> int | None:
        if region is None:
            return None
        elif isinstance(region, str):
            obj = self._backend.regions.get_by_name(region)
            return obj.id
        elif isinstance(region, int):
            return region
        elif isinstance(region, Region):
            return region.id
        else:
            raise ValueError(f"Invalid reference to region: {region}")

    def create(
        self,
        name: str,
        hierarchy: str,
    ) -> Region:
        dto = self._backend.regions.create(name, hierarchy)
        return Region(backend=self._backend, dto=dto)

    def get(self, name: str) -> Region:
        dto = self._backend.regions.get_by_name(name)
        return Region(backend=self._backend, dto=dto)

    def delete(self, x: Region | int | str) -> None:
        if isinstance(x, Region):
            id = x.id
        elif isinstance(x, int):
            id = x
        elif isinstance(x, str):
            dto = self._backend.regions.get_by_name(x)
            id = dto.id
        else:
            raise TypeError("Invalid argument: Must be `Region`, `int` or `str`.")

        self._backend.regions.delete(id)

    def list(self, **kwargs: Unpack[RegionFilter]) -> list[Region]:
        regions = self._backend.regions.list(**kwargs)
        return [Region(backend=self._backend, dto=r) for r in regions]

    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> pd.DataFrame:
        return self._backend.regions.tabulate(**kwargs)

    def get_docs(self, name: str) -> str | None:
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        try:
            return self._backend.regions.docs.get(dimension_id=region_id).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        return self._backend.regions.docs.set(
            dimension_id=region_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        try:
            self._backend.regions.docs.delete(dimension_id=region_id)
            return None
        except DocsModel.NotFound:
            return None

    def list_docs(
        self, id: int | None = None, id__in: Iterable[int] | None = None
    ) -> Iterable[str]:
        return [
            item.description
            for item in self._backend.regions.docs.list(
                dimension_id=id, dimension_id__in=id__in
            )
        ]
