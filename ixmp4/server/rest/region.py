from fastapi import APIRouter, Body, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.region.filter import RegionFilter

from . import deps
from .base import BaseModel
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/regions",
    tags=["regions"],
)


class RegionInput(BaseModel):
    name: str
    hierarchy: str


class EnumerationOutput(BaseModel):
    __root__: list[api.Region] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: RegionFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: RegionFilter = Body(None),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.post("/", response_model=api.Region)
def create(
    region: RegionInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.regions.create(**region.dict())


@router.delete("/{id}/")
def delete(
    id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    backend.regions.delete(id)
