from fastapi import APIRouter, Depends, Query, Body, Path

from ixmp4.data.backend import Backend
from ixmp4.data import api
from ixmp4.data.db.unit.filter import UnitFilter

from .decorators import autodoc
from .base import BaseModel
from . import deps


router: APIRouter = APIRouter(
    prefix="/units",
    tags=["units"],
)


class UnitInput(BaseModel):
    name: str


class EnumerationOutput(BaseModel):
    __root__: api.DataFrame | list[api.Unit]


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: UnitFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: UnitFilter = Body(None),
    table: bool = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.enumerate(_filter=filter, table=bool(table))


@router.post("/", response_model=api.Unit)
def create(
    unit: UnitInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.create(**unit.dict())


@router.delete("/{id}/")
def delete(
    id: int = Path(None),
    backend: Backend = Depends(deps.get_backend),
):
    backend.units.delete(id)
