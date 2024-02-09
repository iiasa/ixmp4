from fastapi import APIRouter, Body, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.region.filter import RegionFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/regions",
    tags=["regions"],
)


class RegionInput(BaseModel):
    name: str
    hierarchy: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Region])
def query(
    filter: RegionFilter = Body(RegionFilter()),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.regions.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.regions.count(_filter=filter),
        pagination=pagination,
    )


@autodoc
@router.post("/", response_model=api.Region)
def create(
    region: RegionInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.create(**region.model_dump())


@router.delete("/{id}/")
def delete(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
):
    backend.regions.delete(id)
