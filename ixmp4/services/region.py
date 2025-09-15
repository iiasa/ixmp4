from typing import Any, Callable, List, TypedDict

import fastapi as fa
import httpx
import pandas as pd
import pydantic as pyd
from sqlalchemy import orm
from toolkit import db
from toolkit.auth import AuthorizationContext
from toolkit.exceptions import NotFound, NotUnique
from typing_extensions import Unpack

from ixmp4.models.region import Region

from .base import AbstractService, procedure
from .dto import DataFrame as DataFrameDTO
from .dto import EnumerationOutput, Pagination


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
    executor: db.r.SessionExecutor
    items: ItemRepository
    pandas: PandasRepository

    def __init__(self, session: orm.Session, auth_ctx: AuthorizationContext):
        self.executor = db.r.SessionExecutor(session)
        self.items = ItemRepository(self.executor)
        self.pandas = PandasRepository(self.executor)
        super().__init__(session, auth_ctx)

    @procedure()
    def create(self, name: str, hierarchy: str) -> RegionDTO:
        self.items.create({"name": name, "hierarchy": hierarchy})
        return RegionDTO.model_validate(self.items.get({"name": name}))

    @create.endpoint()
    @staticmethod
    def create_endpoint(router: fa.APIRouter, svc_dep: Callable[..., Any]) -> None:
        @router.post("/", response_model=RegionDTO)
        def create(
            svc: RegionService = fa.Depends(svc_dep), region: CreateRegion = fa.Body()
        ) -> RegionDTO:
            return svc.create(region["name"], region["hierarchy"])

    @create.client()
    @staticmethod
    def create_client(
        client: httpx.Client,
    ) -> Callable[["RegionService", str, str], RegionDTO]:
        def create(self: "RegionService", name: str, hierarchy: str) -> RegionDTO:
            res = client.post("/", json={"name": name, "hierarchy": hierarchy})
            return RegionDTO(**res.json())

        return create

    @procedure()
    def delete(self, id: int) -> None:
        self.items.delete_by_pk({"id": id})

    @delete.endpoint()
    @staticmethod
    def delete_endpoint(router: fa.APIRouter, svc_dep: Callable[..., Any]) -> None:
        @router.delete("/{id}/")
        def delete(
            svc: RegionService = fa.Depends(svc_dep),
            id: int = fa.Path(),
        ) -> None:
            return svc.delete(id)

    @procedure()
    def get(self, name: str) -> RegionDTO:
        return RegionDTO.model_validate(self.items.get({"name": name}))

    @get.endpoint()
    @staticmethod
    def get_endpoint(router: fa.APIRouter, svc_dep: Callable[..., Any]) -> None:
        @router.patch("/get", response_model=RegionDTO)
        def get(
            svc: RegionService = fa.Depends(svc_dep), region: GetRegion = fa.Body()
        ) -> RegionDTO:
            return svc.get(region["name"])

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
    def list(self, **kwargs: Unpack[RegionFilter]) -> list[Region]:
        return self.items.list(values=kwargs)

    @list.endpoint()
    @staticmethod
    def list_endpoint(
        router: fa.APIRouter, svc_dep: Callable[..., "RegionService"]
    ) -> None:
        @router.patch("/list", response_model=EnumerationOutput[List[RegionDTO]])
        def list(
            svc: RegionService = fa.Depends(svc_dep),
            filter: RegionFilter = fa.Body(None),
            pagination: Pagination = fa.Depends(),
        ) -> EnumerationOutput[List[Region]]:
            return EnumerationOutput(
                results=svc.items.list(
                    values=filter,
                    limit=pagination.limit,
                    offset=pagination.offset,
                ),
                total=svc.items.count(values=filter),
                pagination=pagination,
            )

    @procedure()
    def tabulate(self, **kwargs: Unpack[RegionFilter]) -> pd.DataFrame:
        return self.pandas.tabulate(values=kwargs)

    @tabulate.endpoint()
    @staticmethod
    def tabulate_endpoint(
        router: fa.APIRouter, svc_dep: Callable[..., "RegionService"]
    ) -> None:
        @router.patch("/tabulate", response_model=EnumerationOutput[DataFrameDTO])
        def tabulate(
            svc: RegionService = fa.Depends(svc_dep),
            filter: RegionFilter = fa.Body(None),
            pagination: Pagination = fa.Depends(),
        ) -> EnumerationOutput[pd.DataFrame]:
            return EnumerationOutput(
                results=svc.pandas.tabulate(
                    values=filter,
                    limit=pagination.limit,
                    offset=pagination.offset,
                ),
                total=svc.pandas.count(values=filter),
                pagination=pagination,
            )
