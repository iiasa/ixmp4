from typing import TypedDict

import pandas as pd
import pydantic as pyd
from toolkit import db
from toolkit.exceptions import NotFound, NotUnique
from typing_extensions import Unpack

from ixmp4.models.region import Region

from .base import (
    AbstractService,
    DirectTransport,
    procedure,
)


class RegionDTO(pyd.BaseModel):
    name: str
    hierarchy: str
    id: int

    model_config = pyd.ConfigDict(from_attributes=True)


class CreateRegion(TypedDict):
    name: str
    hierarchy: str


class GetRegion(TypedDict):
    name: str


class RegionNotFound(NotFound):
    pass


class RegionNotUnique(NotUnique):
    pass


class IdFilter(TypedDict, total=False):
    id: int
    id__in: list[int]


class NameFilter(TypedDict, total=False):
    name: str
    name__in: list[str]
    name__like: str
    name__ilike: str
    name__notlike: str
    name__notilike: str


class HierarchyFilter(TypedDict, total=False):
    hierarchy: str
    hierarchy__in: list[str]
    hierarchy__like: str
    hierarchy__ilike: str
    hierarchy__notlike: str
    hierarchy__notilike: str


class RegionFilter(IdFilter, NameFilter, HierarchyFilter, total=False):
    pass


class ItemRepository(db.r.ItemRepository[Region]):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = db.r.ModelTarget(Region)
    filter = db.r.Filter(RegionFilter, Region)


class PandasRepository(db.r.PandasRepository):
    NotFound = RegionNotFound
    NotUnique = RegionNotUnique
    target = db.r.ModelTarget(Region)
    filter = db.r.Filter(RegionFilter, Region)


class RegionService(AbstractService):
    router_prefix = "/regions"
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init_direct__(self, transport: DirectTransport) -> None:
        self.executor = db.r.SessionExecutor(transport.session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)

    @procedure()
    def create(self, name: str, hierarchy: str) -> RegionDTO:
        self.items.create({"name": name, "hierarchy": hierarchy})
        return RegionDTO.model_validate(self.items.get({"name": name}))

    @procedure()
    def delete(self, id: int) -> None:
        self.items.delete_by_pk({"id": id})

    @procedure()
    def get(self, name: str) -> RegionDTO:
        return RegionDTO.model_validate(self.items.get({"name": name}))

    def get_or_create(self, name: str, hierarchy: str | None = None) -> RegionDTO:
        try:
            region = self.get(name)
        except RegionNotFound:
            if hierarchy is None:
                raise TypeError(
                    "Argument `hierarchy` is required if `Region` with `name` does not "
                    "exist."
                )
            return self.create(name, hierarchy)

        if hierarchy is not None and region.hierarchy != hierarchy:
            raise RegionNotUnique(name)
        else:
            return region

    @procedure()
    def list(self, **kwargs: Unpack[RegionFilter]) -> list[RegionDTO]:
        return [RegionDTO.model_validate(i) for i in self.items.list(values=kwargs)]

    @procedure()
    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> pd.DataFrame:
        return self.pandas.tabulate(values=kwargs)
