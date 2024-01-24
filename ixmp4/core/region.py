from datetime import datetime
from typing import Optional, Union

import pandas as pd

from ixmp4.data.abstract import Docs as DocsModel
from ixmp4.data.abstract import Region as RegionModel

from .base import BaseFacade, BaseModelFacade


class Region(BaseModelFacade):
    _model: RegionModel
    NotUnique = RegionModel.NotUnique
    NotFound = RegionModel.NotFound
    DeletionPrevented = RegionModel.DeletionPrevented

    @property
    def id(self) -> int:
        """Unique id."""
        return self._model.id

    @property
    def name(self) -> str:
        """Region name."""
        return self._model.name

    @property
    def hierarchy(self) -> str:
        """Region hierarchy."""
        return self._model.hierarchy

    @property
    def created_at(self) -> datetime | None:
        return self._model.created_at

    @property
    def created_by(self) -> str | None:
        return self._model.created_by

    def delete(self):
        """Deletes the region from the database."""
        self.backend.regions.delete(self._model.id)

    @property
    def docs(self):
        try:
            return self.backend.regions.docs.get(self.id).description
        except DocsModel.NotFound:
            return None

    @docs.setter
    def docs(self, description):
        if description is None:
            self.backend.regions.docs.delete(self.id)
        else:
            self.backend.regions.docs.set(self.id, description)

    @docs.deleter
    def docs(self):
        try:
            self.backend.regions.docs.delete(self.id)
        # TODO: silently failing
        except DocsModel.NotFound:
            return None

    def __str__(self) -> str:
        return f"<Region {self.id} name={self.name}>"


class RegionRepository(BaseFacade):
    def _get_region_id(self, region: Optional[Union[str, int, "Region"]]) -> int | None:
        if region is None:
            return None
        elif isinstance(region, str):
            obj = self.backend.regions.get(region)
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
        model = self.backend.regions.create(name, hierarchy)
        return Region(_backend=self.backend, _model=model)

    def get(self, name: str) -> Region:
        model = self.backend.regions.get(name)
        return Region(_backend=self.backend, _model=model)

    def delete(self, x: Region | int | str):
        if isinstance(x, Region):
            id = x.id
        elif isinstance(x, int):
            id = x
        elif isinstance(x, str):
            model = self.backend.regions.get(x)
            id = model.id
        else:
            raise TypeError("Invalid argument: Must be `Region`, `int` or `str`.")

        self.backend.regions.delete(id)

    def list(
        self,
        name: str | None = None,
        hierarchy: str | None = None,
    ) -> list[Region]:
        regions = self.backend.regions.list(name=name, hierarchy=hierarchy)
        return [Region(_backend=self.backend, _model=r) for r in regions]

    def tabulate(
        self,
        name: str | None = None,
        hierarchy: str | None = None,
    ) -> pd.DataFrame:
        return self.backend.regions.tabulate(name=name, hierarchy=hierarchy)

    def get_docs(self, name: str) -> str | None:
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        try:
            return self.backend.regions.docs.get(dimension_id=region_id).description
        except DocsModel.NotFound:
            return None

    def set_docs(self, name: str, description: str | None) -> str | None:
        if description is None:
            self.delete_docs(name=name)
            return None
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        return self.backend.regions.docs.set(
            dimension_id=region_id, description=description
        ).description

    def delete_docs(self, name: str) -> None:
        # TODO: this function is failing silently, which we should avoid
        region_id = self._get_region_id(name)
        if region_id is None:
            return None
        try:
            self.backend.regions.docs.delete(dimension_id=region_id)
            return None
        except DocsModel.NotFound:
            return None
