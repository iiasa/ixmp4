from fastapi import APIRouter, Body, Depends, Path, Query

from ixmp4.data import api
from ixmp4.data.backend.db import SqlAlchemyBackend as Backend
from ixmp4.data.db.unit.filter import UnitFilter

from . import deps
from .base import BaseModel, EnumerationOutput, Pagination
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/units",
    tags=["units"],
)


class UnitInput(BaseModel):
    name: str


@autodoc
@router.patch("/", response_model=EnumerationOutput[api.Unit])
def query(
    filter: UnitFilter = Body(None),
    table: bool = Query(False),
    pagination: Pagination = Depends(),
    backend: Backend = Depends(deps.get_backend),
):
    return EnumerationOutput(
        results=backend.units.paginate(
            _filter=filter,
            limit=pagination.limit,
            offset=pagination.offset,
            table=bool(table),
        ),
        total=backend.units.count(_filter=filter),
        pagination=pagination,
    )


@router.post("/", response_model=api.Unit)
def create(
    unit: UnitInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.units.create(**unit.model_dump())


@router.delete("/{id}/")
def delete(
    id: int = Path(),
    backend: Backend = Depends(deps.get_backend),
):
    backend.units.delete(id)
