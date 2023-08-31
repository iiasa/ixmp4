from fastapi import APIRouter, Body, Depends, Query

from ixmp4.data import api
from ixmp4.data.backend.base import Backend
from ixmp4.data.db.model.filter import ModelFilter

from . import deps
from .base import BaseModel
from .decorators import autodoc

router: APIRouter = APIRouter(
    prefix="/models",
    tags=["models"],
)


class ModelInput(BaseModel):
    name: str


class EnumerationOutput(BaseModel):
    __root__: list[api.Model] | api.DataFrame


@autodoc
@router.get("/", response_model=EnumerationOutput)
def enumerate(
    filter: ModelFilter = Depends(),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.patch("/", response_model=EnumerationOutput)
def query(
    filter: ModelFilter = Body(None),
    table: bool | None = Query(False),
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.enumerate(_filter=filter, table=bool(table))


@autodoc
@router.post("/", response_model=api.Model)
def create(
    model: ModelInput,
    backend: Backend = Depends(deps.get_backend),
):
    return backend.models.create(**model.dict())
